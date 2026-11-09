import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';

// Use the existing compiler, so tests also run on Node versions without TS loading.
const source = readFileSync(new URL('../src/index.ts', import.meta.url), 'utf8');
const { outputText } = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
});
const { default: bot } = await import(`data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`);
const update = (n, version = '2') => ({ name: `pkg-${n}`, before: '1', after: version, path: `app/pkg-${n}` });
const signature = ({ name, after, path }) => ({ name, after, path });

function harness(t, { rows = [], known = [], users = 0, kvConfig = false, strictRate = false } = {}) {
    let now = 1_800_000_000_000;
    let feed = rows;
    const db = new Map([['last_sigs', JSON.stringify(known.map(signature))]]);
    const writes = new Map();
    const calls = [];
    const sent = [];
    const rejected = [];
    const fail = { put: null, send: null, list: false, get: null, upstream: false };
    t.mock.method(Date, 'now', () => now);
    t.mock.method(globalThis, 'setTimeout', (fn, ms) => { now += ms; queueMicrotask(fn); return 0; });
    t.mock.method(console, 'error', () => {});
    t.mock.method(console, 'warn', () => {});
    const env = {
        TELEGRAM_BOT_TOKEN: 'test-token', TELEGRAM_CHAT_ID: 'channel', SECRET_KEY: 'secret', ADMIN_CHAT_ID: 'admin',
        STATE: {
            async get(key) {
                calls.push(['get', key]);
                if (fail.get?.(key)) throw new Error('KV GET failed');
                return db.get(key) ?? null;
            },
            async put(key, value) {
                calls.push(['put', key]);
                if (fail.put?.(key, value)) throw new Error('KV PUT failed: simulated outage');
                if (strictRate && writes.has(key) && now - writes.get(key) < 1000) {
                    rejected.push(key);
                    throw new Error('KV PUT failed: 429 Too Many Requests');
                }
                writes.set(key, now);
                db.set(key, value);
            },
            async delete(key) { calls.push(['delete', key]); db.delete(key); },
            async list({ prefix, cursor }) {
                calls.push(['list', prefix]);
                if (fail.list) throw new Error('KV LIST failed');
                const keys = [...db.keys()].filter(k => k.startsWith(prefix));
                const start = Number(cursor || 0);
                const end = Math.min(start + 10, keys.length);
                return { keys: keys.slice(start, end).map(name => ({ name })), list_complete: end === keys.length, cursor: String(end) };
            },
        },
    };
    if (kvConfig) {
        for (const key of ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'SECRET_KEY']) {
            db.set(key, env[key]); delete env[key];
        }
    }
    for (let i = 0; i < users; i++) db.set(`subs:user:${i}`, JSON.stringify(Object.fromEntries(rows.map(r => [r.name, true]))));
    t.mock.method(globalThis, 'fetch', async (url, init) => {
        if (String(url).startsWith('https://api.telegram.org/')) {
            const message = JSON.parse(init.body);
            if (fail.send?.(message)) return new Response('test send failure', { status: 503 });
            sent.push(message);
            return new Response('{"ok":true}');
        }
        assert.equal(url, 'https://raw.githubusercontent.com/AOSC-Dev/anicca/main/pkgsupdate.json');
        if (fail.upstream) throw new Error('upstream connection failed');
        return new Response(typeof feed === 'string' ? feed : JSON.stringify(feed));
    });
    async function run(next = feed) {
        feed = next; now += 600_000;
        const pending = [];
        await bot.scheduled({}, env, { waitUntil: p => pending.push(p) });
        await Promise.all(pending);
    }
    async function request(path, command, user = 'user', chatType = 'private') {
        now += 600_000;
        const pending = [];
        const response = await bot.fetch(new Request(`https://bot.test${path}`, {
            method: command ? 'POST' : 'GET',
            headers: { Authorization: 'Bearer secret', 'X-Telegram-Bot-Api-Secret-Token': 'secret' },
            body: command ? JSON.stringify({ message: { text: command, from: { id: user }, chat: { id: user, type: chatType } } }) : undefined,
        }), env, { waitUntil: p => pending.push(p) });
        await Promise.all(pending);
        return response;
    }
    const packages = () => sent.filter(m => m.chat_id === 'channel' && m.text.startsWith('<b>pkg-'));
    const count = (op, key) => calls.filter(([a, b]) => a === op && (key === undefined || b === key)).length;
    return { run, request, db, sent, calls, rejected, fail, packages, count };
}

test('an unchanged snapshot sends the English status without writing, deleting or listing KV', async t => {
    const h = harness(t, { rows: [update(1)], known: [update(1)] });
    await h.run(); // legacy migration may write once
    h.calls.length = 0; h.sent.length = 0;
    await h.run();
    assert.equal(h.sent.length, 1);
    assert.equal(h.sent[0].chat_id, 'channel');
    assert.match(h.sent[0].text, /^<b>[^<]+<\/b>\nTotal updates: <code>1<\/code>\n<i>Nothing New Happened<\/i>$/);
    for (const op of ['put', 'delete', 'list']) assert.equal(h.count(op), 0, op);
    assert(h.count('get') <= 2);
});

test('admin trigger posts the idle status only for a successful check with no new updates', async t => {
    const h = harness(t, { rows: [update(1), update(1)], known: [update(1)] });
    await h.run();
    h.sent.length = 0;
    await h.request('/webhook', '/trigger', 'admin');
    assert.deepEqual(h.sent.filter(m => m.chat_id === 'admin').map(m => m.text), ['已排程执行']);
    assert.deepEqual(h.sent.filter(m => m.chat_id === 'channel').map(m => m.text.split('\n').slice(1).join('\n')), [
        'Total updates: <code>1</code>\nDeduped duplicates: <code>1</code>\n<i>Nothing New Happened</i>',
    ]);
    h.sent.length = 0;
    await h.run('invalid JSON');
    assert.equal(h.sent.some(m => m.chat_id === 'channel'), false);
    await h.run([update(1, '3')]);
    assert.equal(h.packages().length, 1);
    assert.equal(h.sent.some(m => m.text.includes('Nothing New Happened')), false);
});

test('checksub replies privately when there are no subscriptions', async t => {
    const h = harness(t);
    await h.request('/webhook', '/checksub');
    assert.deepEqual(h.sent.map(({chat_id, text}) => [chat_id, text]), [['user', '你目前没有任何订阅。']]);
    assert.deepEqual(h.calls, [['get', 'subs:user:user']]);
});

test('checksub shows only the caller’s packages and reports a feed failure', async t => {
    const h = harness(t, { rows: [update(1), update(3)] });
    h.db.set('subs:user:user', JSON.stringify({ 'pkg-1': true, 'pkg-2': true }));
    h.db.set('subs:user:other', JSON.stringify({ 'pkg-3': true }));
    await h.request('/webhook', '/checksub');
    assert.equal(h.sent.length, 1);
    assert.equal(h.sent[0].chat_id, 'user');
    assert.match(h.sent[0].text, /<b>pkg-1<\/b>\nversion: <code>1<\/code> → <code>2<\/code>/);
    assert.match(h.sent[0].text, /<b>pkg-2<\/b>\n当前暂无上游更新记录。/);
    assert.match(h.sent[0].text, /https:\/\/packages\.aosc\.io\/changelog\/pkg-2/);
    assert.doesNotMatch(h.sent[0].text, /pkg-3/);
    assert.deepEqual(h.calls, [['get', 'subs:user:user']]);

    h.fail.upstream = true;
    await h.request('/webhook', '/checksub');
    assert.equal(h.sent.at(-1).text, '获取订阅详情失败，请稍后再试。');
});

test('checksub keeps group replies free of subscription details and splits long private replies', async t => {
    const h = harness(t);
    const names = Array.from({ length: 30 }, (_, i) => `package-with-a-long-name-${i}`);
    h.db.set('subs:user:user', JSON.stringify(Object.fromEntries(names.map(name => [name, true]))));
    await h.request('/webhook', '/checksub', 'user', 'supergroup');
    assert.equal(h.sent[0].text, '请在与机器人的私聊中使用 /checksub');
    assert.deepEqual(h.calls, []);

    h.sent.length = 0;
    await h.request('/webhook', '/checksub');
    assert(h.sent.length > 1);
    assert(h.sent.every(m => m.chat_id === 'user' && m.text.length <= 3500 && m.text.startsWith('你的订阅详情：\n')));
    const combined = h.sent.map(m => m.text).join('\n');
    for (const name of names) assert(combined.includes(`<b>${name}</b>`));
    assert.deepEqual(h.calls, [['get', 'subs:user:user']]);
});

test('disappearance and empty snapshots never erase already announced versions', async t => {
    const h = harness(t, { rows: [update(1)], known: [update(1)] });
    for (let i = 0; i < 3; i++) { await h.run([]); await h.run([update(1)]); }
    assert.equal(h.packages().length, 0);
    await h.run([update(1, '3')]);
    assert.equal(h.packages().length, 1);
});

test('a 15-package batch respects same-key KV rate limits and is not replayed', async t => {
    const rows = Array.from({ length: 15 }, (_, i) => update(i));
    const h = harness(t, { rows, strictRate: true });
    await h.run();
    assert.equal(h.packages().length, 15);
    assert.deepEqual(h.rejected, []);
    assert(h.count('put') <= 2, 'one lock write plus one notification-state write');
    await h.run();
    assert.equal(h.packages().length, 15);
});

test('an unchanged feed still drains batches beyond the first fifteen', async t => {
    const h = harness(t, { rows: Array.from({ length: 31 }, (_, i) => update(i)) });
    await h.run(); assert.equal(h.packages().length, 15);
    await h.run(); assert.equal(h.packages().length, 30);
    await h.run(); assert.equal(h.packages().length, 31);
    await h.run(); assert.equal(h.packages().length, 31);
});

test('subscriptions are scanned once with pagination and config is read once per invocation', async t => {
    const rows = Array.from({ length: 15 }, (_, i) => update(i));
    const h = harness(t, { rows, users: 20, kvConfig: true, strictRate: true });
    await h.run();
    assert.equal(h.count('list'), 2);
    for (let i = 0; i < 20; i++) assert.equal(h.count('get', `subs:user:${i}`), 1);
    for (const key of ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'SECRET_KEY']) assert.equal(h.count('get', key), 1);
    assert.equal(h.sent.filter(m => m.chat_id !== 'channel').length, 10);
    assert.deepEqual(h.rejected, []);
    assert(h.count('put') <= 2);
    h.calls.length = 0;
    await h.run();
    assert.equal(h.count('list'), 0);
    assert.equal(h.packages().length, 15);
    assert.equal(h.sent.filter(m => m.chat_id !== 'channel').length, 20);
    for (const key of ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'SECRET_KEY']) assert.equal(h.count('get', key), 1);
});

test('a failed channel delivery remains pending, successful earlier messages do not replay', async t => {
    const h = harness(t, { rows: [update(1), update(2), update(3)] });
    h.fail.send = m => m.text.startsWith('<b>pkg-2</b>');
    await h.run(); assert.equal(h.packages().length, 1);
    h.fail.send = null;
    await h.run();
    assert.equal(h.packages().length, 3);
    assert.equal(h.packages().filter(m => m.text.startsWith('<b>pkg-1</b>')).length, 1);
});

test('a transient state-save failure retries the checkpoint without resending Telegram messages', async t => {
    const h = harness(t, { rows: [update(1)], strictRate: true });
    let failures = 0;
    h.fail.put = key => key === 'notification_state' && failures++ === 0;
    await h.run(); await h.run();
    assert.equal(failures, 2);
    assert.equal(h.packages().length, 1);
});

test('old newline and JSON string signature formats still deduplicate', async t => {
    const h = harness(t, { rows: [update(1)] });
    h.db.set('last_sigs', 'pkg-1|2|app/pkg-1');
    await h.run(); assert.equal(h.packages().length, 0);
    h.db.delete('notification_state');
    h.db.set('last_sigs', JSON.stringify(['pkg-1|2|app/pkg-1']));
    await h.run(); assert.equal(h.packages().length, 0);
});

test('corrupt persisted signatures fail closed instead of replaying the feed', async t => {
    const h = harness(t, { rows: [update(1)] });
    h.db.set('last_sigs', '[broken');
    await h.run();
    assert.equal(h.packages().length, 0);
    assert.equal(h.db.get('last_sigs'), '[broken');
});

test('legacy private queue drains even if the upstream feed is unavailable', async t => {
    const h = harness(t);
    h.db.set('subs_queue', JSON.stringify([{ chatId: 'subscriber', text: 'pending', enqueuedAt: 1 }]));
    await h.run('invalid JSON'); await h.run('invalid JSON');
    assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, 1);
});

test('failed private messages retry up to the existing limit without replaying successful ones', async t => {
    const h = harness(t);
    h.db.set('subs_queue', JSON.stringify([
        { chatId: 'ok', text: 'pending', enqueuedAt: 1 },
        { chatId: 'fail', text: 'pending', enqueuedAt: 1 },
    ]));
    let attempts = 0;
    h.fail.send = m => m.chat_id === 'fail' && (++attempts > 0);
    for (let i = 0; i < 6; i++) await h.run();
    assert.equal(attempts, 4);
    assert.equal(h.sent.filter(m => m.chat_id === 'ok').length, 1);
});

test('explicit force still resends and the status endpoint reflects the new checkpoint', async t => {
    const h = harness(t, { rows: [update(1)], known: [update(1)] });
    const response = await h.request('/trigger?sync=1&force=1');
    assert.equal(response.status, 200);
    assert.equal(h.packages().length, 1);
    const status = await (await h.request('/')).json();
    assert(status.lastHash); assert(status.lastTs);
});

test('batch subscribe writes once and repeated subscribe/unsubscribe no-ops do not write', async t => {
    const h = harness(t, { strictRate: true });
    h.db.set('repo_pkg_names', JSON.stringify({ updatedAt: Date.now(), names: ['pkg-1', 'pkg-2', 'pkg-3'] }));
    await h.request('/webhook', '/subscribe pkg-1;pkg-2;pkg-3');
    assert.deepEqual(JSON.parse(h.db.get('subs:user:user')), { 'pkg-1': true, 'pkg-2': true, 'pkg-3': true });
    assert.equal(h.count('get', 'subs:user:user'), 1);
    assert.equal(h.count('put', 'subs:user:user'), 1);
    assert.deepEqual(h.rejected, []);
    h.calls.length = 0;
    await h.request('/webhook', '/subscribe pkg-1;pkg-2');
    await h.request('/webhook', '/unsubscribe missing');
    assert.equal(h.count('put', 'subs:user:user'), 0);
});

test('force_updatedb retains history and pending private notifications without sending updates', async t => {
    const h = harness(t, { rows: [update(1)], known: [update(2)] });
    h.db.set('subs_queue', JSON.stringify([{ chatId: 'subscriber', text: 'pending', enqueuedAt: 1 }]));
    await h.request('/webhook', '/force_updatedb', 'admin');
    assert.equal(h.packages().length, 0);
    await h.run([update(1), update(2)]);
    assert.equal(h.packages().length, 0);
    assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, 1);
});

test('a corrupt new checkpoint never falls back to stale legacy signatures', async t => {
    const h = harness(t, { rows: [update(1)] });
    h.db.set('notification_state', '{broken');
    await h.run();
    assert.equal(h.packages().length, 0);
    assert.equal(h.db.get('notification_state'), '{broken');
});

test('exhausted KV writes prevent processing before channel delivery', async t => {
    const h = harness(t, { rows: [update(1)] });
    h.fail.put = () => true;
    await h.run();
    assert.equal(h.packages().length, 0);
});

test('a permanent checkpoint failure is reported and never retried by resending within the run', async t => {
    const h = harness(t, { rows: [update(1)] });
    h.fail.put = key => key === 'notification_state';
    const response = await h.request('/trigger?sync=1');
    assert.equal(response.status, 500);
    assert.equal((await response.json()).ok, false);
    assert.equal(h.count('put', 'notification_state'), 3);
    assert.equal(h.packages().length, 1);
});

for (const failure of ['list', 'get']) {
    test(`existing private queue drains when subscriber ${failure} fails; new channel updates wait`, async t => {
        const h = harness(t, { rows: [update(1)], users: 1 });
        h.db.set('subs_queue', JSON.stringify([{ chatId: 'subscriber', text: 'pending', enqueuedAt: 1 }]));
        if (failure === 'list') h.fail.list = true;
        else h.fail.get = key => key === 'subs:user:0';
        await h.run();
        assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, 1);
        assert.equal(h.packages().length, 0);
        h.fail.list = false; h.fail.get = null;
        await h.run();
        assert.equal(h.packages().length, 1);
        assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, 1);
    });
}

const legacyFormats = {
    objects: entries => JSON.stringify(entries.map(signature)),
    strings: entries => JSON.stringify(entries.map(e => `${e.name}|${e.after}|${e.path}`)),
    lines: entries => entries.map(e => `${e.name}|${e.after}|${e.path}`).join('\n'),
};

for (const [format, encode] of Object.entries(legacyFormats)) {
    test(`migration preserves ${format} history, a multi-batch queue, and subscription records`, async t => {
        const known = [update(1), { ...update(3), path: 'app/path|with|pipes' }];
        const h = harness(t, { rows: [update(1), update(2)], strictRate: true, kvConfig: true });
        const queue = Array.from({ length: 23 }, (_, i) => ({ chatId: `legacy-${i}`, text: `pending-${i}`, enqueuedAt: 100 + i, attempts: i === 22 ? 2 : 0 }));
        const legacy = new Map([
            ['last_sigs', encode(known)], ['subs_queue', JSON.stringify(queue)],
            ['last_hash', 'old-hash'], ['last_ts', '123456789'],
            ['subs:user:alice', '{ "pkg-1": true, "pkg-2": true }'],
            ['subs:user:bob', '{"pkg-2":true,"pkg-3":true}'],
            ['subs:user:empty', '{}'],
        ]);
        for (const [key, value] of legacy) h.db.set(key, value);
        const config = new Map(['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'SECRET_KEY'].map(key => [key, h.db.get(key)]));
        await h.run();
        assert.equal(h.packages().length, 1);
        assert(h.packages()[0].text.startsWith('<b>pkg-2</b>'));
        const checkpoint = JSON.parse(h.db.get('notification_state'));
        assert.deepEqual(checkpoint.signatures, [...known.map(signature), signature(update(2))]);
        assert.deepEqual(checkpoint.queue.slice(0, 13), queue.slice(10));
        assert.deepEqual(checkpoint.queue.slice(13).map(m => m.chatId), ['alice', 'bob']);
        await h.run(); await h.run(); await h.run();
        for (const item of queue) assert.equal(h.sent.filter(m => m.chat_id === item.chatId && m.text === item.text).length, 1);
        for (const user of ['alice', 'bob']) assert.equal(h.sent.filter(m => m.chat_id === user && m.text.startsWith('<b>pkg-2</b>')).length, 1);
        assert.equal(h.packages().length, 1);
        assert.deepEqual(JSON.parse(h.db.get('notification_state')).queue, []);
        for (const [key, value] of [...legacy, ...config]) assert.equal(h.db.get(key), value, `unchanged ${key}`);
        assert.deepEqual(h.rejected, []);
        h.calls.length = 0;
        await h.run([update(1), update(2), known[1]]);
        assert.equal(h.packages().length, 1);
        assert.deepEqual(h.calls, [
            ['get', 'notification_state'],
            ...[...config.keys()].map(key => ['get', key]),
        ]);
    });
}

test('migration retains retry counts instead of granting old queue items a fresh retry budget', async t => {
    const h = harness(t);
    const queue = [{ chatId: 'subscriber', text: 'retry', enqueuedAt: 1, attempts: 2 }];
    h.db.set('subs_queue', JSON.stringify(queue));
    let attempts = 0;
    h.fail.send = m => m.chat_id === 'subscriber' && ++attempts;
    await h.run();
    assert.deepEqual(JSON.parse(h.db.get('notification_state')).queue, [{ ...queue[0], attempts: 3 }]);
    await h.run(); await h.run();
    assert.equal(attempts, 2);
    assert.deepEqual(JSON.parse(h.db.get('notification_state')).queue, []);
});

test('a migrated checkpoint takes precedence over even unreadable legacy records', async t => {
    const h = harness(t, { rows: [update(1)], known: [update(1)] });
    await h.run();
    for (const key of ['last_sigs', 'subs_queue', 'last_hash', 'last_ts']) h.db.set(key, '{stale');
    h.fail.get = key => ['last_sigs', 'subs_queue', 'last_hash', 'last_ts'].includes(key);
    await h.run([update(1), update(2)]); await h.run();
    assert.equal(h.packages().length, 1);
    assert(h.packages()[0].text.startsWith('<b>pkg-2</b>'));
});

for (const key of ['last_sigs', 'subs_queue', 'last_hash', 'last_ts']) {
    test(`failed legacy ${key} read does not send or overwrite partial migration data`, async t => {
        const h = harness(t, { rows: [update(1), update(2)], known: [update(1)], users: 1 });
        h.db.set('subs_queue', JSON.stringify([{ chatId: 'subscriber', text: 'pending', enqueuedAt: 1 }]));
        const original = new Map(h.db);
        h.fail.get = k => k === key;
        await h.run();
        assert.equal(h.packages().length, 0);
        assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, 0);
        assert.equal(h.db.has('notification_state'), false);
        for (const [k, value] of original) assert.equal(h.db.get(k), value);
        h.fail.get = null;
        await h.run(); await h.run();
        assert.equal(h.packages().length, 1);
        assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, 1);
        assert.equal(h.sent.filter(m => m.chat_id === '0').length, 1);
    });
}

test('ambiguous migration PUT success retries the same checkpoint without replay or queue loss', async t => {
    const h = harness(t, { rows: [update(1), update(2)], known: [update(1)] });
    h.db.set('subs_queue', JSON.stringify(Array.from({ length: 12 }, (_, i) => ({ chatId: 'subscriber', text: `pending-${i}`, enqueuedAt: i + 1 }))));
    const checkpoints = [];
    h.fail.put = (key, value) => {
        if (key !== 'notification_state') return false;
        checkpoints.push(value);
        if (checkpoints.length !== 1) return false;
        h.db.set(key, value); // the storage write succeeds, but its response is lost
        return true;
    };
    await h.run(); await h.run(); await h.run();
    assert.equal(checkpoints[0], checkpoints[1]);
    assert.equal(h.packages().length, 1);
    for (let i = 0; i < 12; i++) assert.equal(h.sent.filter(m => m.chat_id === 'subscriber' && m.text === `pending-${i}`).length, 1);
    assert.deepEqual(JSON.parse(h.db.get('notification_state')).queue, []);
});

for (const pending of [false, true]) {
    test(`upstream connection failure is reported with ${pending ? 'pending' : 'empty'} private queue`, async t => {
        const h = harness(t, { rows: [update(1)], known: [update(1)] });
        await h.run();
        if (pending) {
            const state = JSON.parse(h.db.get('notification_state'));
            state.queue.push({ chatId: 'subscriber', text: 'pending', enqueuedAt: 1 });
            h.db.set('notification_state', JSON.stringify(state));
        }
        h.fail.upstream = true;
        const response = await h.request('/trigger?sync=1');
        assert.equal(response.status, 500);
        assert.equal((await response.json()).ok, false);
        assert.match(JSON.parse(h.db.get('last_error')).message, /upstream connection failed/);
        assert(h.sent.some(m => m.chat_id === 'admin' && m.text.includes('upstream connection failed')));
        assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, pending ? 1 : 0);
        assert.deepEqual(JSON.parse(h.db.get('notification_state')).queue, []);
        h.fail.upstream = false;
        await h.run();
        assert.equal(h.packages().length, 0);
        assert.equal(h.sent.filter(m => m.chat_id === 'subscriber').length, pending ? 1 : 0);
    });
}
