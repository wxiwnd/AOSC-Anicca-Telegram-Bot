import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const HELP = `Usage: npm run backup:kv -- [--account-id ID] [--namespace-id ID] [--out DIR] [--execute]

Preview by default. To export, add --execute and set CLOUDFLARE_API_TOKEN
with Workers KV Storage Read access. The output directory must be new.

IDs may also come from CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_KV_NAMESPACE_ID.
See DEPLOYMENT.md for backup and deployment steps.`;

function readOptions(args, env) {
    let values;
    try {
        ({ values } = parseArgs({
            args,
            options: {
                'account-id': { type: 'string' },
                'namespace-id': { type: 'string' },
                out: { type: 'string' },
                execute: { type: 'boolean' },
                help: { type: 'boolean', short: 'h' },
            },
        }));
    } catch {
        throw new Error('Invalid arguments. See --help.');
    }
    if (values.help) return { help: true };
    const { execute = false } = values;
    const accountId = values['account-id'] ?? env.CLOUDFLARE_ACCOUNT_ID ?? '';
    const namespaceId = values['namespace-id'] ?? env.CLOUDFLARE_KV_NAMESPACE_ID ?? '';
    for (const [name, id] of [['account-id', accountId], ['namespace-id', namespaceId]]) {
        if ((id || execute) && !/^[a-f0-9]{32}$/i.test(id)) {
            throw new Error(`${name} must be a 32-character hex ID.`);
        }
    }
    if (values.out === '') throw new Error('--out requires a directory.');
    const out = resolve(values.out ?? `.backups/kv-${new Date().toISOString().replace(/[:.]/g, '-')}`);
    return { accountId, namespaceId, out, execute };
}

async function exportNamespace({ accountId, namespaceId, out: output }, token, fetchImpl) {
    await mkdir(dirname(output), { recursive: true, mode: 0o700 });
    await mkdir(output, { mode: 0o700 });
    const manifestPath = join(output, 'manifest.json');
    const manifest = {
        formatVersion: 1,
        complete: false,
        accountId,
        namespaceId,
        startedAt: new Date().toISOString(),
        completedAt: null,
        listedKeyCount: 0,
        entries: [],
    };
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2), { flag: 'wx', mode: 0o600 });
    const base = `https://api.cloudflare.com/client/v4/accounts/${accountId}/storage/kv/namespaces/${namespaceId}`;
    async function get(url, label) {
        let response;
        try {
            response = await fetchImpl(url, {
                method: 'GET',
                headers: { Authorization: `Bearer ${token}` },
                redirect: 'error',
                signal: AbortSignal.timeout(30_000),
            });
        } catch {
            throw new Error(`${label} failed (network error or timeout).`);
        }
        if (!response.ok) throw new Error(`${label} failed (HTTP ${response.status}).`);
        return response;
    }
    try {
        let cursor = '';
        const keys = [];
        const seenCursors = new Set();
        do {
            const url = new URL(`${base}/keys`);
            url.searchParams.set('limit', '1000');
            if (cursor) url.searchParams.set('cursor', cursor);
            const response = await get(url, 'Listing KV keys');
            let page;
            try { page = await response.json(); }
            catch { throw new Error('Invalid JSON in KV key list.'); }
            if (page.success !== true || !Array.isArray(page.result)) throw new Error('Invalid KV key list.');
            for (const key of page.result) {
                if (typeof key?.name !== 'string') throw new Error('Invalid key in KV key list.');
                keys.push(key);
            }
            manifest.listedKeyCount = keys.length;
            cursor = page.result_info?.cursor ?? '';
            if (typeof cursor !== 'string' || (cursor && seenCursors.has(cursor))) throw new Error('Invalid KV pagination cursor.');
            if (cursor) seenCursors.add(cursor);
        } while (cursor);

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const response = await get(`${base}/values/${encodeURIComponent(key.name)}`, `Reading KV value ${i + 1}`);
            let bytes;
            try { bytes = Buffer.from(await response.arrayBuffer()); }
            catch { throw new Error(`Incomplete response for KV value ${i + 1}.`); }
            const file = `${String(i).padStart(6, '0')}.bin`;
            await writeFile(join(output, file), bytes, { flag: 'wx', mode: 0o600 });
            manifest.entries.push({
                name: key.name,
                file,
                bytes: bytes.length,
                sha256: createHash('sha256').update(bytes).digest('hex'),
                ...(key.expiration !== undefined ? { expiration: key.expiration } : {}),
                ...(key.metadata !== undefined ? { metadata: key.metadata } : {}),
            });
        }
        manifest.completedAt = new Date().toISOString();
        manifest.complete = true;
    } finally {
        await writeFile(manifestPath, JSON.stringify(manifest, null, 2));
    }
    return manifest.entries.length;
}

export async function main(args, { env = process.env, fetchImpl = globalThis.fetch, log = console.log, error = console.error } = {}) {
    try {
        const options = readOptions(args, env);
        if (options.help) { log(HELP); return 0; }
        if (!options.execute) {
            log(`Backup preview: account=${options.accountId || '(unset)'} namespace=${options.namespaceId || '(unset)'} output=${options.out}`);
            return 0;
        }
        const token = env.CLOUDFLARE_API_TOKEN?.trim();
        if (!token) throw new Error('Set CLOUDFLARE_API_TOKEN before running with --execute.');
        const count = await exportNamespace(options, token, fetchImpl);
        log(`Backed up ${count} keys to ${options.out}.`);
        return 0;
    } catch (e) {
        const token = env.CLOUDFLARE_API_TOKEN?.trim();
        const message = e instanceof Error ? e.message : 'Backup failed.';
        error(token ? message.replaceAll(token, '[redacted]') : message);
        return 1;
    }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
    process.exitCode = await main(process.argv.slice(2));
}
