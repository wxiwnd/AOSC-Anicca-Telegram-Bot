import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, readdir, rm, stat, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { main } from '../scripts/backup-kv.mjs';

const accountId = 'a'.repeat(32);
const namespaceId = 'b'.repeat(32);
const token = 'test-api-token-do-not-print';
const env = { CLOUDFLARE_ACCOUNT_ID: accountId, CLOUDFLARE_KV_NAMESPACE_ID: namespaceId, CLOUDFLARE_API_TOKEN: token };
const base = `https://api.cloudflare.com/client/v4/accounts/${accountId}/storage/kv/namespaces/${namespaceId}`;
const page = (result, cursor = '') => Response.json({ success: true, result, result_info: { cursor } });

async function fixture(t) {
    const root = await mkdtemp(join(tmpdir(), 'anicca-kv-test-'));
    t.after(() => rm(root, { recursive: true, force: true }));
    const out = join(root, 'backup');
    const messages = [];
    const args = ['--out', out];
    const fetchImpl = t.mock.fn(() => { throw new Error('Unexpected network request'); });
    return { root, out, messages, args, options: { env, fetchImpl, log: m => messages.push(m), error: m => messages.push(m) } };
}

test('backup default preview does not fetch, create files, or require credentials', async t => {
    const f = await fixture(t);
    assert.equal(await main(f.args, { ...f.options, env: {} }), 0);
    assert.equal(f.options.fetchImpl.mock.callCount(), 0);
    assert.deepEqual(await readdir(f.root), []);
    assert(f.messages.some(m => m.includes('preview')));
});

test('help with execute never fetches and never prints a token', async t => {
    const f = await fixture(t);
    assert.equal(await main([...f.args, '--execute', '--help'], f.options), 0);
    assert.equal(f.options.fetchImpl.mock.callCount(), 0);
    assert.deepEqual(await readdir(f.root), []);
    assert(!f.messages.join('\n').includes(token));
});

test('execution requires real IDs and a token before any network or file creation', async t => {
    const f = await fixture(t);
    for (const invalidEnv of [{}, { ...env, CLOUDFLARE_API_TOKEN: '' }, { ...env, CLOUDFLARE_KV_NAMESPACE_ID: 'REPLACE_ME' }]) {
        assert.equal(await main([...f.args, '--execute'], { ...f.options, env: invalidEnv }), 1);
    }
    assert.equal(f.options.fetchImpl.mock.callCount(), 0);
    assert.deepEqual(await readdir(f.root), []);
});

test('exports every paginated key with exact binary bytes, metadata and expiration using only GET', async t => {
    const f = await fixture(t);
    const entries = [
        { name: 'last_sigs' },
        { name: 'subs:user:123', metadata: { owner: 'test' }, expiration: 1_900_000_000 },
        { name: 'notification_state', metadata: null },
        { name: '路径/../../odd?#name' },
        { name: 'TELEGRAM_BOT_TOKEN' },
    ];
    const bytes = [Buffer.from('[]'), Buffer.from('{"pkg":true}'), Buffer.alloc(0), Buffer.from([0, 255, 10, 13]), Buffer.from('secret-bot-token')];
    const requests = [];
    const fetchImpl = async (input, options) => {
        const url = new URL(input);
        requests.push(url);
        assert.equal(options.method, 'GET');
        assert.equal(options.headers.Authorization, `Bearer ${token}`);
        assert.equal(url.origin, 'https://api.cloudflare.com');
        if (url.pathname.endsWith('/keys')) {
            assert.equal(url.searchParams.get('limit'), '1000');
            return url.searchParams.has('cursor') ? page(entries.slice(2)) : page(entries.slice(0, 2), 'next+/=');
        }
        const key = decodeURIComponent(url.pathname.split('/values/')[1]);
        const i = entries.findIndex(e => e.name === key);
        assert(i >= 0);
        return new Response(bytes[i]);
    };
    assert.equal(await main([...f.args, '--execute'], { ...f.options, fetchImpl }), 0);
    const manifest = JSON.parse(await readFile(join(f.out, 'manifest.json'), 'utf8'));
    assert.equal(manifest.complete, true);
    assert.equal(manifest.accountId, accountId);
    assert.equal(manifest.namespaceId, namespaceId);
    assert.equal(manifest.listedKeyCount, 5);
    assert.equal(manifest.entries.length, 5);
    assert(manifest.completedAt);
    assert.equal(requests.length, 7);
    assert.equal(requests[1].searchParams.get('cursor'), 'next+/=');
    for (let i = 0; i < entries.length; i++) {
        const saved = manifest.entries[i];
        assert.equal(saved.name, entries[i].name);
        assert.equal(saved.bytes, bytes[i].length);
        assert.equal(saved.sha256, createHash('sha256').update(bytes[i]).digest('hex'));
        assert.equal(saved.expiration, entries[i].expiration);
        assert.deepEqual(saved.metadata, entries[i].metadata);
        assert.match(saved.file, /^\d{6}\.bin$/);
        assert.deepEqual(await readFile(join(f.out, saved.file)), bytes[i]);
    }
    if (process.platform !== 'win32') {
        assert.equal((await stat(f.out)).mode & 0o777, 0o700);
        for (const file of await readdir(f.out)) assert.equal((await stat(join(f.out, file))).mode & 0o777, 0o600);
    }
    const output = f.messages.join('\n');
    assert(!output.includes(token));
    assert(!output.includes('secret-bot-token'));
});

test('an existing backup directory is not overwritten or followed by network requests', async t => {
    const f = await fixture(t);
    await mkdir(f.out);
    await writeFile(join(f.out, 'keep'), 'original');
    assert.equal(await main([...f.args, '--execute'], f.options), 1);
    assert.equal(f.options.fetchImpl.mock.callCount(), 0);
    assert.deepEqual(await readdir(f.out), ['keep']);
    assert.equal(await readFile(join(f.out, 'keep'), 'utf8'), 'original');
});

for (const status of [403, 404, 429, 500]) {
    test(`HTTP ${status} leaves an incomplete manifest and never claims a usable backup`, async t => {
        const f = await fixture(t);
        let reads = 0;
        const fetchImpl = async input => {
            if (new URL(input).pathname.endsWith('/keys')) return page([{ name: 'one' }, { name: 'two' }]);
            if (reads++ === 0) return new Response('first value');
            return new Response(token, { status });
        };
        assert.equal(await main([...f.args, '--execute'], { ...f.options, fetchImpl }), 1);
        const manifest = JSON.parse(await readFile(join(f.out, 'manifest.json'), 'utf8'));
        assert.equal(manifest.complete, false);
        assert.equal(manifest.completedAt, null);
        assert.equal(manifest.listedKeyCount, 2);
        assert.equal(manifest.entries.length, 1);
        assert.equal(await readFile(join(f.out, manifest.entries[0].file), 'utf8'), 'first value');
        assert(f.messages.join('\n').includes(String(status)));
        assert(!f.messages.join('\n').includes(token));
    });
}

test('empty namespaces produce a complete empty backup', async t => {
    const f = await fixture(t);
    assert.equal(await main([...f.args, '--execute'], { ...f.options, fetchImpl: async () => page([]) }), 0);
    const manifest = JSON.parse(await readFile(join(f.out, 'manifest.json'), 'utf8'));
    assert.equal(manifest.complete, true);
    assert.deepEqual(manifest.entries, []);
});

test('API errors and invalid list responses fail without disclosing server content', async t => {
    const f = await fixture(t);
    for (const [i, response] of [Response.json({ success: false, errors: [{ message: token }] }), new Response(token)].entries()) {
        const out = join(f.root, `invalid-${i}`);
        assert.equal(await main(['--out', out, '--execute'], { ...f.options, fetchImpl: async () => response }), 1);
        assert.equal(JSON.parse(await readFile(join(out, 'manifest.json'), 'utf8')).complete, false);
    }
    assert(!f.messages.join('\n').includes(token));
});

test('network failure preserves an incomplete manifest without leaking the thrown error', async t => {
    const f = await fixture(t);
    assert.equal(await main([...f.args, '--execute'], { ...f.options, fetchImpl: async () => { throw new Error(token); } }), 1);
    assert.equal(JSON.parse(await readFile(join(f.out, 'manifest.json'), 'utf8')).complete, false);
    assert(!f.messages.join('\n').includes(token));
});

test('repeated pagination cursors stop instead of endlessly consuming quota', async t => {
    const f = await fixture(t);
    let calls = 0;
    assert.equal(await main([...f.args, '--execute'], { ...f.options, fetchImpl: async () => { calls++; return page([], 'same'); } }), 1);
    assert.equal(calls, 2);
});

test('CLI IDs override environment values and unknown flags never execute', async t => {
    const f = await fixture(t);
    assert.equal(await main([...f.args, '--account-id', accountId, '--namespace-id', namespaceId], { ...f.options, env: {} }), 0);
    assert(f.messages.join('\n').includes(namespaceId));
    assert.equal(await main([...f.args, '--execute', '--unknown'], f.options), 1);
    assert.equal(f.options.fetchImpl.mock.callCount(), 0);
    assert.deepEqual(await readdir(f.root), []);
});
