export interface Env {
    STATE: KVNamespace;
    TELEGRAM_BOT_TOKEN?: string;
    TELEGRAM_CHAT_ID?: string;
    FETCH_URL?: string;
    SECRET_KEY?: string;
    ADMIN_CHAT_ID?: string;
}

const DEFAULT_URL = "https://raw.githubusercontent.com/AOSC-Dev/anicca/main/pkgsupdate.json";
const KV_LAST_HASH = "last_hash";
const KV_LAST_TS = "last_ts";
const KV_LAST_SIGS = "last_sigs";
const KV_ADMIN_CHAT_ID = "ADMIN_CHAT_ID";
const KV_LAST_ERROR = "last_error"; // JSON: { ts, where, message, stack?, detail? }
const KV_SUBS_PREFIX = "subs:user:"; // subs:user:<userId> => { [packageLower: string]: true }
const KV_PROCESSING_LOCK = "processing_lock";
const KV_SUBS_QUEUE = "subs_queue"; // legacy queue, read during migration only
const KV_NOTIFICATION_STATE = "notification_state";
const KV_REPO_PACKAGE_NAMES = "repo_pkg_names";
const MAX_PER_RUN = 15;
const SLEEP_BETWEEN_MS = 250;
const PROCESSING_LOCK_TTL_SECONDS = 240;
const PROCESSING_LOCK_MAX_RETRIES = 3;
const PROCESSING_LOCK_RETRY_BASE_DELAY_MS = 500;
const SUB_QUEUE_MAX_PER_RUN = 10;
const SUB_QUEUE_SLEEP_MS = 400;
const SUB_QUEUE_RETRY_LIMIT = 3;
const CHECKSUB_MESSAGE_LIMIT = 3500;
const CHECKSUB_PACKAGES_PER_MESSAGE = 25;
const REPO_BASE_URL = "https://repo.aosc.io/debs/dists/stable";
const REPO_PACKAGE_PATHS = ["main/binary-all/Packages", "main/binary-amd64/Packages"];
const REPO_PKG_LIST_MAX_AGE_MS = 6 * 60 * 60 * 1000;

type SigEntry = { name: string; after: string; path: string };
type SubQueueItem = { chatId: string; text: string; enqueuedAt: number; attempts?: number };
type NotificationState = {
    version: 1;
    signatures: SigEntry[];
    queue: SubQueueItem[];
    lastHash: string | null;
    lastTs: string | null;
};

function toSigEntry(u: any): SigEntry {
    return {
        name: String(u?.name ?? ""),
        after: String(u?.after ?? ""),
        path: String(u?.path ?? ""),
    };
}

function sigEntryKey(entry: SigEntry): string {
    return `${entry.name}\u0000${entry.after}\u0000${entry.path}`;
}

function sigEntryKeyFromUpdate(u: any): string {
    return sigEntryKey(toSigEntry(u));
}

function parseLegacySigString(raw: string): SigEntry | null {
    if (!raw) return null;
    const parts = raw.split("|");
    if (parts.length === 0) return null;
    const name = parts.shift() ?? "";
    const after = parts.shift() ?? "";
    const path = parts.join("|");
    return { name, after, path };
}

function coerceSigEntry(input: any): SigEntry | null {
    if (!input && input !== "") return null;
    if (typeof input === "string") return parseLegacySigString(input);
    if (typeof input === "object") {
        const name = typeof input.name === "string" ? input.name : "";
        const after = typeof input.after === "string" ? input.after : "";
        const path = typeof input.path === "string" ? input.path : "";
        return { name, after, path };
    }
    return null;
}

function createLockToken(): string {
    if (typeof globalThis.crypto !== "undefined" && typeof globalThis.crypto.randomUUID === "function") {
        return globalThis.crypto.randomUUID();
    }
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function acquireProcessingLock(env: Env, ttlSeconds: number): Promise<string | null> {
    const now = Date.now();
    try {
        const existingRaw = await env.STATE.get(KV_PROCESSING_LOCK);
        if (existingRaw) {
            try {
                const existing = JSON.parse(existingRaw);
                const lastTs = Number(existing?.ts) || 0;
                if (lastTs && now - lastTs < ttlSeconds * 1000) {
                    console.warn("acquireProcessingLock: existing fresh lock detected");
                    return null;
                }
            } catch (err) {
                console.warn("acquireProcessingLock: stale lock entry parse error", err);
            }
        }
        const token = createLockToken();
        const payload = JSON.stringify({ token, ts: now });
        await env.STATE.put(KV_PROCESSING_LOCK, payload, { expirationTtl: ttlSeconds });

        const maxAttempts = 4;
        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            const confirmRaw = await env.STATE.get(KV_PROCESSING_LOCK);
            if (confirmRaw) {
                try {
                    const confirm = JSON.parse(confirmRaw);
                    if (confirm?.token === token) return token;
                } catch (err) {
                    console.warn("acquireProcessingLock: confirmation parse error", err);
                }
            }
            if (attempt < maxAttempts - 1) await sleep(150 * (attempt + 1));
        }
        console.warn("acquireProcessingLock: failed to confirm lock write");
    } catch (e) {
        console.error("acquireProcessingLock failed", e);
    }
    return null;
}

async function releaseProcessingLock(env: Env, token: string): Promise<void> {
    try {
        const raw = await env.STATE.get(KV_PROCESSING_LOCK);
        if (!raw) return;
        try {
            const data = JSON.parse(raw);
            if (data?.token !== token) return;
        } catch {
            return;
        }
        await env.STATE.delete(KV_PROCESSING_LOCK);
    } catch (e) {
        console.error("releaseProcessingLock failed", e);
    }
}

function hashString(s: string): string {
    // Simple FNV-1a 32-bit
    let h = 0x811c9dc5;
    for (let i = 0; i < s.length; i++) {
        h ^= s.charCodeAt(i);
        h = Math.imul(h, 0x01000193) >>> 0;
    }
    return ("00000000" + h.toString(16)).slice(-8);
}

function htmlEscape(s: string): string {
    return s
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

function truncate(s: string, max: number): string {
    if (!s) return s;
    if (s.length <= max) return s;
    return s.slice(0, Math.max(0, max - 1)) + "…";
}

function dedupeUpdates(items: any[]): { list: any[]; dropped: number } {
    if (!Array.isArray(items) || items.length <= 1) {
        return { list: items || [], dropped: 0 };
    }
    const seen = new Set<string>();
    const result: any[] = [];
    for (const it of items) {
        const sig = sigEntryKeyFromUpdate(it);
        if (seen.has(sig)) continue;
        seen.add(sig);
        result.push(it);
    }
    return { list: result, dropped: items.length - result.length };
}

async function forceUpdateDatabase(env: Env): Promise<{ total: number; unique: number; duplicatesRemoved: number; hash: string }> {
    const token = await acquireProcessingLock(env, PROCESSING_LOCK_TTL_SECONDS);
    if (!token) throw new Error("notification processing is already running");
    try {
        const data = await fetchUpdates(env);
        if (!data) throw new Error("failed to fetch updates");
        const { state } = await loadNotificationState(env);
        const { list, dropped } = dedupeUpdates(data.items);
        const known = new Set(state.signatures.map(sigEntryKey));
        for (const item of list) {
            const entry = toSigEntry(item);
            if (!known.has(sigEntryKey(entry))) {
                state.signatures.push(entry);
                known.add(sigEntryKey(entry));
            }
        }
        state.lastHash = hashString(data.raw);
        state.lastTs = String(Date.now());
        await saveNotificationState(env, state);
        return { total: data.items.length, unique: list.length, duplicatesRemoved: dropped, hash: state.lastHash };
    } finally {
        await releaseProcessingLock(env, token);
    }
}

function packageLinks(name: string): string {
    const encoded = encodeURIComponent(name);
    return `links: <a href="https://packages.aosc.io/packages/${encoded}">PackageInfo</a> | <a href="https://packages.aosc.io/changelog/${encoded}">Changelog</a>`;
}

function renderUpdateLines(updates: any[]): string[] {
    const lines: string[] = [];
    for (const u of updates) {
        const nameRaw = String(u.name ?? "(unknown)");
        const beforeRaw = String(u.before ?? "?");
        const afterRaw = String(u.after ?? "?");
        const pathRaw = String(u.path ?? "");

        const name = htmlEscape(truncate(nameRaw, 120));
        const before = htmlEscape(truncate(beforeRaw, 200));
        const after = htmlEscape(truncate(afterRaw, 200));
        const path = htmlEscape(truncate(pathRaw, 800));

        lines.push(`<b>${name}</b>`);
        lines.push(`version: <code>${before}</code> → <code>${after}</code>`);
        if (path) lines.push(`path: <code>${path}</code>`);
        const nameForUrl = String(u.name ?? "").trim();
        if (nameForUrl) lines.push(packageLinks(nameForUrl));
    }
    return lines;
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function recordError(env: Env, where: string, err: any): Promise<void> {
    try {
        let detail = "";
        try {
            detail = JSON.stringify(err);
        } catch {
            detail = String(err);
        }
        const payload = {
            ts: Date.now(),
            where,
            message: String((err && (err.message || err.toString && err.toString())) || err || ""),
            stack: err && (err.stack || undefined),
            detail,
        };
        await env.STATE.put(KV_LAST_ERROR, JSON.stringify(payload));
    } catch (e) {
        console.error("recordError failed", e);
    }
}

async function notifyAdminError(env: Env, where: string, err: any): Promise<void> {
    try {
        const adminId = await getAdminId(env);
        if (!adminId) return;
        const ts = new Date().toISOString();
        const msg = String((err && (err.message || err.toString && err.toString())) || err || "");
        const stack = err && err.stack ? String(err.stack) : "";
        const text = `<b>ERR</b> <code>${htmlEscape(where)}</code> @ <b>${htmlEscape(ts)}</b>\n` +
            `msg: <code>${htmlEscape(truncate(msg, 1000))}</code>` +
            (stack ? `\nstack: <code>${htmlEscape(truncate(stack, 1000))}</code>` : "");
        await sendTelegram(env, text, adminId);
    } catch (e) {
        console.error("notifyAdminError failed", e);
    }
}

// Keep historical signatures: absence from one upstream snapshot is not a new release.
function parseSignatures(raw: string | null): SigEntry[] {
    if (!raw?.trim()) return [];
    const trimmed = raw.trim();
    const values = trimmed.startsWith("[") ? JSON.parse(trimmed) : trimmed.split("\n").filter(Boolean);
    if (!Array.isArray(values)) throw new Error("invalid stored signatures");
    return values.map((value) => {
        if (typeof value === "string" && value.split("|").length < 3) throw new Error("invalid legacy signature");
        if (typeof value === "object" && (!value || [value.name, value.after, value.path].some(v => typeof v !== "string"))) {
            throw new Error("invalid stored signature");
        }
        const entry = coerceSigEntry(value);
        if (!entry) throw new Error("invalid stored signature");
        return entry;
    });
}

function parseQueue(values: unknown): SubQueueItem[] {
    if (!Array.isArray(values)) throw new Error("invalid stored subscription queue");
    return values.map(value => {
        const item = normalizeSubQueueItem(value);
        if (!item) throw new Error("invalid stored subscription message");
        return item;
    });
}

async function loadNotificationState(env: Env): Promise<{ state: NotificationState; legacy: boolean }> {
    const raw = await env.STATE.get(KV_NOTIFICATION_STATE);
    if (raw !== null) {
        const state = JSON.parse(raw);
        if (state?.version !== 1 || !Array.isArray(state.signatures)
            || !(state.lastHash === null || typeof state.lastHash === "string")
            || !(state.lastTs === null || typeof state.lastTs === "string")) {
            throw new Error("invalid notification state");
        }
        return { state: { ...state, signatures: parseSignatures(JSON.stringify(state.signatures)), queue: parseQueue(state.queue) }, legacy: false };
    }
    const [sigs, queue, lastHash, lastTs] = await Promise.all([
        env.STATE.get(KV_LAST_SIGS), env.STATE.get(KV_SUBS_QUEUE),
        env.STATE.get(KV_LAST_HASH), env.STATE.get(KV_LAST_TS),
    ]);
    return {
        state: { version: 1, signatures: parseSignatures(sigs), queue: parseQueue(queue ? JSON.parse(queue) : []), lastHash, lastTs },
        legacy: true,
    };
}

async function saveNotificationState(env: Env, state: NotificationState): Promise<void> {
    const payload = JSON.stringify(state);
    // Retry the checkpoint, never the already-sent Telegram batch. Space retries
    // beyond KV's one-write-per-second limit, including an ambiguous failed PUT.
    for (let attempt = 0; ; attempt++) {
        try {
            await env.STATE.put(KV_NOTIFICATION_STATE, payload);
            return;
        } catch (e) {
            if (attempt >= 2) throw e;
            await sleep(1100 * (attempt + 1));
        }
    }
}

function normalizeSubQueueItem(raw: any): SubQueueItem | null {
    if (!raw || typeof raw !== "object") return null;
    const chatId = String(raw.chatId ?? "").trim();
    const text = typeof raw.text === "string" ? raw.text : "";
    if (!chatId || !text) return null;
    const attempts = Number(raw.attempts ?? 0) || 0;
    const enqueuedAtRaw = Number(raw.enqueuedAt ?? Date.now());
    const enqueuedAt = Number.isFinite(enqueuedAtRaw) ? enqueuedAtRaw : Date.now();
    return { chatId, text, attempts, enqueuedAt };
}

async function drainSubscriptionQueue(env: Env, queue: SubQueueItem[]): Promise<{ queue: SubQueueItem[]; error?: unknown }> {
    const batch = queue.slice(0, SUB_QUEUE_MAX_PER_RUN);
    const rest = queue.slice(batch.length);
    let error: unknown;
    for (let i = 0; i < batch.length; i++) {
        const item = batch[i];
        try {
            await sendTelegram(env, item.text, item.chatId);
        } catch (e) {
            error = e;
            const attempts = (item.attempts ?? 0) + 1;
            if (attempts <= SUB_QUEUE_RETRY_LIMIT) rest.push({ ...item, attempts });
        }
        if (i < batch.length - 1) await sleep(SUB_QUEUE_SLEEP_MS);
    }
    return { queue: rest, error };
}

// List all user IDs that have subscription records
async function listSubscriptionUserIds(env: Env): Promise<string[]> {
    const ids: string[] = [];
    let cursor: string | undefined = undefined;
    while (true) {
        const res: any = await (env.STATE as any).list({ prefix: KV_SUBS_PREFIX, cursor });
        const keys = Array.isArray(res?.keys) ? res.keys : [];
        for (const k of keys) {
            const name = String(k.name || "");
            if (name.startsWith(KV_SUBS_PREFIX)) {
                ids.push(name.slice(KV_SUBS_PREFIX.length));
            }
        }
        if (res?.list_complete || !res?.cursor) break;
        cursor = res.cursor;
    }
    return Array.from(new Set(ids));
}

async function loadUserSubscriptions(env: Env, userId: string): Promise<Record<string, true>> {
    const raw = await env.STATE.get(KV_SUBS_PREFIX + String(userId));
    if (!raw) return {};
    try {
        const obj = JSON.parse(raw);
        if (obj && typeof obj === "object") return obj as Record<string, true>;
    } catch {}
    return {};
}

async function saveUserSubscriptions(env: Env, userId: string, data: Record<string, true>): Promise<void> {
    await env.STATE.put(KV_SUBS_PREFIX + String(userId), JSON.stringify(data));
}

async function addUserSubscriptions(env: Env, userId: string, packages: string[]): Promise<{ added: number; already: number }> {
    if (packages.length === 0) return { added: 0, already: 0 };
    const data = await loadUserSubscriptions(env, userId);
    let added = 0, already = 0;
    for (const name of packages) {
        if (data[name]) {
            already++;
        } else {
            data[name] = true;
            added++;
        }
    }
    if (added > 0) await saveUserSubscriptions(env, userId, data);
    return { added, already };
}

async function loadRepoPackageNameSet(env: Env): Promise<Set<string> | null> {
    try {
        const raw = await env.STATE.get(KV_REPO_PACKAGE_NAMES);
        if (raw) {
            try {
                const parsed = JSON.parse(raw);
                const updatedAt = Number(parsed?.updatedAt || 0);
                const now = Date.now();
                const namesRaw = parsed?.names;
                if (updatedAt && now - updatedAt <= REPO_PKG_LIST_MAX_AGE_MS && Array.isArray(namesRaw)) {
                    const s = new Set<string>();
                    for (const n of namesRaw) {
                        if (typeof n === "string" && n.trim()) {
                            s.add(n.trim().toLowerCase());
                        }
                    }
                    if (s.size > 0) return s;
                }
            } catch (e) {
                await recordError(env, "loadRepoPkgNames.parseKV", e);
            }
        }
    } catch (e) {
        await recordError(env, "loadRepoPkgNames.readKV", e);
    }

    const names = new Set<string>();
    for (const rel of REPO_PACKAGE_PATHS) {
        const url = `${REPO_BASE_URL}/${rel}`;
        try {
            const res = await fetch(url);
            if (!res.ok) continue;
            const text = await res.text();
            if (!text) continue;
            const lines = text.split("\n");
            for (const line of lines) {
                if (line.startsWith("Package:")) {
                    const pkg = line.slice("Package:".length).trim();
                    if (pkg) names.add(pkg.toLowerCase());
                }
            }
        } catch (e) {
            await recordError(env, "loadRepoPkgNames.fetch", { url, error: String(e || "") });
        }
    }
    if (names.size === 0) return null;

    try {
        const payload = JSON.stringify({
            updatedAt: Date.now(),
            names: Array.from(names),
        });
        await env.STATE.put(KV_REPO_PACKAGE_NAMES, payload);
    } catch (e) {
        await recordError(env, "loadRepoPkgNames.saveKV", e);
    }

    return names;
}

// Build this once per batch, including all KV list pages.
async function loadSubscriberIndex(env: Env): Promise<Map<string, string[]>> {
    const index = new Map<string, string[]>();
    for (const uid of await listSubscriptionUserIds(env)) {
        const subscriptions = await loadUserSubscriptions(env, uid);
        for (const name of Object.keys(subscriptions)) {
            if (!subscriptions[name]) continue;
            const users = index.get(name) || [];
            users.push(uid);
            index.set(name, users);
        }
    }
    return index;
}

async function removeUserSubscription(env: Env, userId: string, pkgLower: string): Promise<{ removed: boolean; size: number } > {
    const data = await loadUserSubscriptions(env, userId);
    const existed = !!data[pkgLower];
    if (existed) {
        delete data[pkgLower];
        await saveUserSubscriptions(env, userId, data);
    }
    return { removed: existed, size: Object.keys(data).length };
}

async function getAdminId(env: Env): Promise<string | null> {
    const fromEnv = (env.ADMIN_CHAT_ID || "").trim();
    if (fromEnv) return fromEnv;
    const fromKV = await env.STATE.get(KV_ADMIN_CHAT_ID);
    return fromKV || null;
}

type Config = { token: string | null; chatId: string | null; secret: string | null };
// Each entry point clones Env, so this cache expires with the invocation and
// changed secrets/configuration are picked up on the next request.
const configCache = new WeakMap<Env, Promise<Config>>();
function getConfig(env: Env): Promise<Config> {
    let config = configCache.get(env);
    if (!config) {
        config = readConfig(env);
        configCache.set(env, config);
    }
    return config;
}

async function readConfig(env: Env): Promise<Config> {
    // Secrets/Variable overrides
    const tokenEnv = (env.TELEGRAM_BOT_TOKEN || "").trim();
    const chatEnv = (env.TELEGRAM_CHAT_ID || "").trim();
    const secretEnv = (env.SECRET_KEY || "").trim();

    if (tokenEnv && chatEnv && secretEnv) return { token: tokenEnv, chatId: chatEnv, secret: secretEnv };

    // Read from KV namespace
    const [tokenKV, chatKV, secretKV] = await Promise.all([
        tokenEnv ? Promise.resolve<string | null>(null) : env.STATE.get("TELEGRAM_BOT_TOKEN"),
        chatEnv ? Promise.resolve<string | null>(null) : env.STATE.get("TELEGRAM_CHAT_ID"),
        secretEnv ? Promise.resolve<string | null>(null) : env.STATE.get("SECRET_KEY"),
    ]);

    return {
        token: tokenEnv || tokenKV || null,
        chatId: chatEnv || chatKV || null,
        secret: secretEnv || secretKV || null,
    };
}

async function sendTelegram(env: Env, text: string, chatIdOverride?: string) {
    const { token, chatId } = await getConfig(env);
    const targetChat = chatIdOverride || chatId || "";
    if (!token || !targetChat) {
        throw new Error("ERR: No Telegram token or chat_id provided");
    }
    const url = `https://api.telegram.org/bot${token}/sendMessage`;
    const payload = { chat_id: targetChat, text, parse_mode: "HTML", disable_web_page_preview: true };
    const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload) });
    const body = await res.text().catch(() => "");
    if (!res.ok) {
        throw new Error(`Telegram API ERROR: ${res.status}: ${body}`);
    }
}

async function safeSendTelegram(env: Env, text: string, chatIdOverride?: string): Promise<void> {
    try {
        await sendTelegram(env, text, chatIdOverride);
    } catch (e) {
        console.error("safeSendTelegram error", e);
    }
}

function sendNoUpdates(env: Env, total: number, duplicates: number) {
    const timestamp = new Date().toISOString();
    const deduped = duplicates > 0 ? `\nDeduped duplicates: <code>${duplicates}</code>` : "";
    return sendTelegram(env, `<b>${timestamp}</b>\nTotal updates: <code>${total}</code>${deduped}\n<i>Nothing New Happened</i>`);
}

async function fetchUpdates(env: Env): Promise<{ raw: string; items: any[] } | null> {
    const url = env.FETCH_URL || DEFAULT_URL;
    const res = await fetch(url);
    if (!res.ok) {
        try {
            await res.text();
        } catch {}
        return null;
    }
    const raw = await res.text();
    try {
        const items = JSON.parse(raw);
        if (!Array.isArray(items)) return null;
        return { raw, items };
    } catch {
        return null;
    }
}

async function sendSubscriptionDetails(env: Env, userId: string, chatId: string): Promise<void> {
    const subscriptions = await loadUserSubscriptions(env, userId);
    const names = Object.keys(subscriptions).sort();
    if (names.length === 0) {
        await safeSendTelegram(env, "你目前没有任何订阅。", chatId);
        return;
    }

    const feed = await fetchUpdates(env);
    if (!feed) {
        await safeSendTelegram(env, "无法取得资料，请稍后再试。", chatId);
        return;
    }
    const updates = new Map<string, any>();
    for (const item of feed.items) {
        const name = String(item?.name ?? "").toLowerCase();
        if (Object.hasOwn(subscriptions, name)) updates.set(name, item);
    }

    const title = "你的订阅详情：\n";
    let message = title;
    let count = 0;
    for (const name of names) {
        const update = updates.get(name);
        const block = update
            ? renderUpdateLines([update]).join("\n")
            : `<b>${htmlEscape(name)}</b>\n当前暂无上游更新记录。\n${packageLinks(name)}`;
        if (count > 0 && (message.length + 2 + block.length > CHECKSUB_MESSAGE_LIMIT || count === CHECKSUB_PACKAGES_PER_MESSAGE)) {
            await safeSendTelegram(env, message, chatId);
            message = title;
            count = 0;
        }
        if (count > 0) message += "\n\n";
        message += block;
        count++;
    }
    await safeSendTelegram(env, message, chatId);
}

async function processAndNotify(env: Env, force = false): Promise<{ changed: boolean; count: number; hash?: string; messages?: string[]; skipped?: boolean; reason?: string; duplicatesRemoved?: number }> {
    let lockToken: string | null = null;
    try {
        // Read before taking the lock: idle cron invocations need no KV writes.
        // A failed fetch must not prevent an existing private queue from draining.
        let upstreamError: unknown;
        const data = await fetchUpdates(env).catch((e) => {
            upstreamError = e;
            return null;
        });
        const { list: items, dropped } = dedupeUpdates(data?.items || []);
        let loaded = await loadNotificationState(env);
        const hasNew = () => {
            const known = new Set(loaded.state.signatures.map(sigEntryKey));
            return items.some(item => force || !known.has(sigEntryKeyFromUpdate(item)));
        };
        if (!loaded.legacy && !hasNew() && loaded.state.queue.length === 0) {
            if (upstreamError) throw upstreamError;
            if (data && !force) await sendNoUpdates(env, items.length, dropped);
            return { changed: false, count: items.length, hash: data ? hashString(data.raw) : undefined, duplicatesRemoved: dropped };
        }
        for (let attempt = 0; attempt < PROCESSING_LOCK_MAX_RETRIES; attempt++) {
            lockToken = await acquireProcessingLock(env, PROCESSING_LOCK_TTL_SECONDS);
            if (lockToken) break;
            if (attempt < PROCESSING_LOCK_MAX_RETRIES - 1) await sleep(PROCESSING_LOCK_RETRY_BASE_DELAY_MS * (attempt + 1));
        }
        if (!lockToken) return { changed: false, count: 0, skipped: true, reason: "locked" };
        // Re-read after acquiring the existing lock in case another run finished.
        loaded = await loadNotificationState(env);
        const state = loaded.state;
        const known = new Set(state.signatures.map(sigEntryKey));
        const fresh = force ? items : items.filter(item => !known.has(sigEntryKeyFromUpdate(item)));
        const batch = fresh.slice(0, MAX_PER_RUN);
        const hadQueue = state.queue.length > 0;
        const messages: string[] = [];
        let sendError: unknown;
        if (batch.length > 0) {
            // Read subscribers before sending so a failed KV scan cannot lose DMs
            // for channel messages that have already been checkpointed.
            const header = `<b>${htmlEscape(new Date().toISOString())}</b>\nSummary:\nTotal: <code>${items.length}</code>\nThis batch: <code>${batch.length}</code>\nRemaining: <code>${fresh.length - batch.length}</code>`;
            try {
                const subscribers = await loadSubscriberIndex(env);
                await sendTelegram(env, header);
                for (let i = 0; i < batch.length; i++) {
                    const item = batch[i];
                    const text = renderUpdateLines([item]).join("\n");
                    await sendTelegram(env, text);
                    messages.push(text);
                    const entry = toSigEntry(item);
                    if (!known.has(sigEntryKey(entry))) {
                        state.signatures.push(entry);
                        known.add(sigEntryKey(entry));
                    }
                    for (const chatId of subscribers.get(String(item.name || "").toLowerCase()) || []) {
                        state.queue.push({ chatId, text, enqueuedAt: Date.now() });
                    }
                    if (i < batch.length - 1) await sleep(SLEEP_BETWEEN_MS);
                }
            } catch (e) {
                sendError = e;
            }
        }
        const drained = await drainSubscriptionQueue(env, state.queue);
        state.queue = drained.queue;
        if (loaded.legacy || messages.length > 0 || hadQueue) {
            if (data) state.lastHash = hashString(data.raw);
            state.lastTs = String(Date.now());
            // Signatures and pending DMs share one checkpoint, written once per
            // batch instead of once per package or queue operation.
            await saveNotificationState(env, state);
        }
        // Report source outages after checkpointing any delivered pending DMs.
        if (upstreamError) throw upstreamError;
        const error = sendError || drained.error;
        if (error) {
            await recordError(env, sendError ? "sendUpdate" : "subsQueueSend", error);
            await notifyAdminError(env, sendError ? "sendUpdate" : "subsQueueSend", error);
        }
        if (data && !force && fresh.length === 0) await sendNoUpdates(env, items.length, dropped);
        return { changed: messages.length > 0, count: items.length, hash: data ? hashString(data.raw) : undefined, messages: force ? messages : undefined, duplicatesRemoved: dropped };
    } catch (e) {
        await recordError(env, "processAndNotify", e);
        await notifyAdminError(env, "processAndNotify", e);
        return { changed: false, count: 0, skipped: true, reason: "processing_failed" };
    } finally {
        if (lockToken) await releaseProcessingLock(env, lockToken);
    }
}

async function auth(request: Request, env: Env): Promise<boolean> {
    const { secret } = await getConfig(env);
    if (!secret) return false;
    const url = new URL(request.url);
    return url.searchParams.get("key") === secret || request.headers.get("Authorization") === `Bearer ${secret}`;
}

// Verify Telegram webhook via secret token header.
// When setting the webhook, pass secret_token=SECRET_KEY so Telegram will include
// X-Telegram-Bot-Api-Secret-Token on each request.
async function authTelegram(request: Request, env: Env): Promise<boolean> {
    const { secret } = await getConfig(env);
    if (!secret) return false;
    const header = request.headers.get("X-Telegram-Bot-Api-Secret-Token");
    if (header && header === secret) return true;
    // Optional fallback for manual testing
    const url = new URL(request.url);
    if (url.searchParams.get("key") === secret) return true;
    return false;
}

// Parser
function parseCommand(text: string, botUsername?: string): { command: string; args: string[]; rest: string } {
    const MAX_LEN = 4096;
    if (!text) return { command: "", args: [], rest: "" };
    const raw = text.slice(0, MAX_LEN).trim();

    if (!raw.startsWith("/")) return { command: "", args: [], rest: raw };

    const firstSpace = raw.indexOf(" ");
    const head = (firstSpace === -1 ? raw : raw.slice(0, firstSpace)).trim();
    let cmd = head;

    // Ignore @bot_name
    if (botUsername && cmd.toLowerCase().includes("@")) {
        const [c, at] = [cmd.split("@")[0], cmd.split("@")[1]];
        if (at && ("@" + at).toLowerCase() === ("@" + botUsername).toLowerCase()) {
            cmd = c;
        }
    } else if (cmd.includes("@")) {
        cmd = cmd.split("@")[0];
    }

    cmd = cmd.toLowerCase();

    const restPart = firstSpace === -1 ? "" : raw.slice(firstSpace + 1).trim();

    const args: string[] = [];
    let i = 0;
    while (i < restPart.length) {
        // Ignore whitespace
        while (i < restPart.length && /\s/.test(restPart[i])) i++;
        if (i >= restPart.length) break;

        let ch = restPart[i];
        if (ch === '"' || ch === "'") {
            const quote = ch;
            i++;
            let buf = "";
            let escaped = false;
            while (i < restPart.length) {
                const c = restPart[i++];
                if (escaped) {
                    buf += c;
                    escaped = false;
                } else if (c === "\\") {
                    escaped = true;
                } else if (c === quote) {
                    break;
                } else {
                    buf += c;
                }
            }
            args.push(buf);
        } else {
            let start = i;
            while (i < restPart.length && !/\s/.test(restPart[i])) i++;
            args.push(restPart.slice(start, i));
        }
    }

    const rest = restPart;

    return { command: cmd, args, rest };
}

const PUBLIC_COMMANDS = new Set<string>(["/new_member", "/findupd", "/subscribe", "/unsubscribe", "/listsub", "/checksub", "/help"]);

export default {
    async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
        env = { ...env };
        ctx.waitUntil((async () => {
            try {
                await processAndNotify(env);
            } catch (e) {
                await recordError(env, "scheduled", e);
                await notifyAdminError(env, "scheduled", e);
                console.error(e);
            }
        })());
    },

    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        env = { ...env };
        const url = new URL(request.url);
        if (url.pathname === "/") {
            const { state: { lastHash, lastTs } } = await loadNotificationState(env);
            return new Response(JSON.stringify({ ok: true, lastHash, lastTs, source: env.FETCH_URL || DEFAULT_URL }), { headers: { "content-type": "application/json" } });
        }

        if (url.pathname === "/trigger") {
            if (!(await auth(request, env))) return new Response("Unauthorized", { status: 401 });
            const force = url.searchParams.get("force") === "1";
            const sync = url.searchParams.get("sync") === "1";
            try {
                if (sync) {
                    const res = await processAndNotify(env, force);
                    const failed = res.reason === "processing_failed";
                    return new Response(JSON.stringify({ ok: !failed, mode: "sync", ...res }, null, 2), { status: failed ? 500 : 200, headers: { "content-type": "application/json" } });
                }
                ctx.waitUntil(processAndNotify(env, force));
                return new Response(JSON.stringify({ ok: true, mode: "async", scheduled: true, force }, null, 2), { status: 202, headers: { "content-type": "application/json" } });
            } catch (e: any) {
                await recordError(env, "/trigger", e);
                await notifyAdminError(env, "/trigger", e);
                return new Response(JSON.stringify({ ok: false, error: String(e?.message || e) }), { status: 500, headers: { "content-type": "application/json" } });
            }
        }

        if (url.pathname === "/webhook") {
            // Telegram need a 200 OK reply
            if (request.method !== "POST") {
                return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
            }
            try {
                // Verify Telegram secret header to prevent spoofed calls
                if (!(await authTelegram(request, env))) {
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                let update: any;
                try {
                    update = await request.json();
                } catch (e) {
                    console.error("webhook json parse error", e);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                const adminId = await getAdminId(env);
                const msg = update?.message;
                const chatId = String(msg?.chat?.id || "");
                const chatType = String(msg?.chat?.type || "");
                const text = String(msg?.text || "").trim();
                const { command, args, rest } = parseCommand(text);

                // Ensure message exists
                if (!msg) {
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }

                // Allow commands in PUBLIC_COMMANDS to be triggered by anyone (any chat).
                // For all other commands, enforce admin-only (private chat + adminId).
                if (!PUBLIC_COMMANDS.has(command)) {
                    if (!adminId || chatType !== "private" || chatId !== String(adminId)) {
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                }

                const help = "可用命令:\n"
                    + "/help - 显示此帮助信息\n"
                    + "/debug - 显示详细 Debug 信息(admin)\n"
                    + "/send &lt;text&gt; - 发送一则信息到预设频道(admin)\n"
                    + "/trigger - 立即触发抓取推送(admin)\n"
                    + "/force_updatedb - 强制将上游版本记录为最新并写入 KV(admin)\n"
                    + "/findupd &lt;package-name&gt; - 查找可能存在的上游更新\n"
                    + "/subscribe &lt;package-name[;package2;...]&gt; - 订阅指定软件包更新\n"
                    + "/unsubscribe &lt;package-name&gt; - 取消订阅指定软件包\n"
                    + "/listsub [package-name] - 列出我的订阅\n"
                    + "/checksub - 查看已订阅软件包的上游更新详情";

                if (command === "/start" || command === "/help") {
                    await safeSendTelegram(env, help, chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/debug") {
                    const { state } = await loadNotificationState(env);
                    const { lastHash, lastTs } = state;
                    const sigCount = new Set(state.signatures.map(sigEntryKey)).size;
                    const lastErrRaw = await env.STATE.get(KV_LAST_ERROR);
                    let lastErrInfo = "null";
                    if (lastErrRaw) {
                        try {
                            const le = JSON.parse(lastErrRaw);
                            const when = new Date(le.ts || Date.now()).toISOString();
                            const where = htmlEscape(String(le.where || ""));
                            const msg = htmlEscape(truncate(String(le.message || ""), 500));
                            lastErrInfo = `time: <code>${when}</code> where: <code>${where}</code>\nmsg: <code>${msg}</code>`;
                        } catch {
                            lastErrInfo = `<code>${htmlEscape(truncate(lastErrRaw, 500))}</code>`;
                        }
                    }
                    const info = `lastHash: <code>${htmlEscape(String(lastHash || ""))}</code>\nlastTs: <code>${htmlEscape(String(lastTs || ""))}</code>\nsignatures: <code>${sigCount}</code>`;
                    const infoWithErr = info + `\nlastError:\n${lastErrInfo}`;
                    await safeSendTelegram(env, infoWithErr, chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/send") {
                    const payload = (rest || args.join(" ")).trim();
                    if (!payload) {
                        await safeSendTelegram(env, "格式: /send &lt;text&gt;", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    await safeSendTelegram(env, payload);
                    await safeSendTelegram(env, "已发送。", chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/trigger") {
                    ctx.waitUntil(processAndNotify(env));
                    await safeSendTelegram(env, "已排程执行", chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/force_updatedb") {
                    try {
                        const info = await forceUpdateDatabase(env);
                        const extra = info.duplicatesRemoved > 0 ? `\n去重: <code>${info.duplicatesRemoved}</code>` : "";
                        const body = `<b>已刷新数据库</b>\n总计: <code>${info.total}</code>\n唯一记录: <code>${info.unique}</code>${extra}\nhash: <code>${htmlEscape(info.hash)}</code>`;
                        await safeSendTelegram(env, body, chatId);
                    } catch (e) {
                        await recordError(env, "/force_updatedb", e);
                        await safeSendTelegram(env, `刷新失败：<code>${htmlEscape(truncate(String(e || ""), 400))}</code>`, chatId);
                    }
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/new_member") {
                    const welcomeMessage = "每个人进来都会啰嗦一下：这个群是贡献者交流群，基本谈工作但偶尔会水，但是注意这里可能会讨论不便公开甚至涉及 NDA 的内容，因此原则上不允许转发此群任何内容";
                    await safeSendTelegram(env, welcomeMessage, chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/subscribe") {
                    if (chatType !== "private") {
                        await safeSendTelegram(env, "请在与机器人的私聊中使用 /subscribe", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    const raw = (rest || args.join(" ") || "").trim();
                    if (!raw) {
                        await safeSendTelegram(env, "格式: /subscribe <package1;package2;...>", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    // Split by half/full-width semicolons
                    const parts = raw.split(/[;；]/).map(s => s.trim()).filter(Boolean);
                    const uniqueLowers: string[] = [];
                    const seen = new Set<string>();
                    for (const p of parts) {
                        const pl = p.toLowerCase();
                        if (!seen.has(pl)) { seen.add(pl); uniqueLowers.push(pl); }
                    }
                    if (uniqueLowers.length === 0) {
                        await safeSendTelegram(env, "未提供有效的包名。", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    // Validate existence against repository package list if available;
                    // fall back to accepting all provided names when the list is unavailable.
                    const repoSet = await loadRepoPackageNameSet(env);
                    const valids: string[] = [];
                    const invalids: string[] = [];
                    if (repoSet && repoSet.size > 0) {
                        for (const pl of uniqueLowers) {
                            if (repoSet.has(pl)) valids.push(pl); else invalids.push(pl);
                        }
                    } else {
                        for (const pl of uniqueLowers) {
                            valids.push(pl);
                        }
                    }
                    const userId = String(msg?.from?.id || "");
                    let added = 0, already = 0;
                    try {
                        ({ added, already } = await addUserSubscriptions(env, userId, valids));
                    } catch (e) {
                        await recordError(env, "subscribe.add", e);
                        await safeSendTelegram(env, "保存订阅失败，请稍后重试。", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    const lines: string[] = [];
                    lines.push(`订阅结果：`);
                    if (valids.length > 0) {
                        lines.push(`有效包(${valids.length}): ` + valids.map(x => `<code>${htmlEscape(x)}</code>`).join(", "));
                        lines.push(`新增 ${added} | 已存在 ${already}`);
                    }
                    if (invalids.length > 0) {
                        lines.push(`未找到下列软件包：`);
                        lines.push(invalids.map(x => `<code>${htmlEscape(x)}</code>`).join(", "));
                    }
                    await safeSendTelegram(env, lines.join("\n"), chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/unsubscribe") {
                    if (chatType !== "private") {
                        await safeSendTelegram(env, "请在与机器人的私聊中使用 /unsubscribe", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    const query = (rest || args.join(" ") || "").trim();
                    if (!query) {
                        await safeSendTelegram(env, "格式: /unsubscribe <package-name>", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    const pkgLower = query.toLowerCase();
                    const userId = String(msg?.from?.id || "");
                    try {
                        const resRem = await removeUserSubscription(env, userId, pkgLower);
                        if (resRem.removed) {
                            await safeSendTelegram(env, `已取消订阅: <code>${htmlEscape(query)}</code>`, chatId);
                        } else {
                            await safeSendTelegram(env, `未找到您的订阅: <code>${htmlEscape(query)}</code>`, chatId);
                        }
                    } catch (e) {
                        await recordError(env, "/unsubscribe", e);
                        await safeSendTelegram(env, `取消订阅失败：${htmlEscape(String(e || ""))}`, chatId);
                    }
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/listsub") {
                    if (chatType !== "private") {
                        await safeSendTelegram(env, "请在与机器人的私聊中使用 /listsub", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    const query = (rest || args.join(" ") || "").trim().toLowerCase();
                    try {
                        const userId = String(msg?.from?.id || "");
                        const data = await loadUserSubscriptions(env, userId);
                        const keys = Object.keys(data).sort();
                        if (keys.length === 0) {
                            await safeSendTelegram(env, "你目前没有任何订阅。", chatId);
                            return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                        }
                        const filter = query || "";
                        const selected = filter ? keys.filter((k) => k.includes(filter)) : keys;
                        if (selected.length === 0) {
                            await safeSendTelegram(env, `未找到符合 <code>${htmlEscape(filter)}</code> 的订阅。`, chatId);
                            return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                        }
                        const lines: string[] = ["你的订阅："]; 
                        for (const k of selected) {
                            lines.push(`- <code>${htmlEscape(k)}</code>`);
                        }
                        await safeSendTelegram(env, lines.join("\n"), chatId);
                    } catch (e) {
                        await recordError(env, "/listsub", e);
                        await safeSendTelegram(env, `列出订阅失败：${htmlEscape(String(e || ""))}`, chatId);
                    }
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/checksub") {
                    if (chatType !== "private") {
                        await safeSendTelegram(env, "请在与机器人的私聊中使用 /checksub", chatId);
                    } else {
                        try {
                            await sendSubscriptionDetails(env, String(msg?.from?.id || ""), chatId);
                        } catch (e) {
                            console.error("/checksub failed", e);
                            await safeSendTelegram(env, "获取订阅详情失败，请稍后再试。", chatId);
                        }
                    }
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/findupd") {
                    const query = (rest || args.join(" ") || "").trim();
                    if (!query) {
                        await safeSendTelegram(env, "格式: /findupd &lt;package-name&gt; 基于软件包仓库进行上游更新查询（仅列出前五项）", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    try {
                        const data = await fetchUpdates(env);
                        if (!data) {
                            await safeSendTelegram(env, `无法取得资料，请稍后再试。`, chatId);
                            return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                        }
                        const items: any[] = data.items || [];
                        // Find items whose name equals or contains the query (case-insensitive)
                        const q = query.toLowerCase();
                        const matched = items.filter((it: any) => String(it.name || "").toLowerCase().includes(q));
                        if (matched.length === 0) {
                            await safeSendTelegram(env, `找不到符合 "${htmlEscape(query)}" 的软件包。`, chatId);
                            return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                        }
                        // Prepare up to 5 results and format using renderUpdateLines to match channel push
                        const top = matched.slice(0, 5);
                        const parts: string[] = [];
                        parts.push("搜索到以下软件包存在上游更新：");
                        for (const it of top) {
                            const rendered = renderUpdateLines([it]).join("\n");
                            parts.push(rendered);
                        }
                        await safeSendTelegram(env, parts.join("\n\n"), chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    } catch (e) {
                        await recordError(env, "/findupd", e);
                        await safeSendTelegram(env, `搜寻失败：${htmlEscape(String(e || ""))}`, chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                }
                await safeSendTelegram(env, "未知命令，输入 /help 查看用法", chatId);
                return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
            } catch (e) {
                await recordError(env, "/webhook", e);
                await notifyAdminError(env, "/webhook", e);
                console.error("/webhook handler error", e);
                return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
            }
        }

        return new Response("Not Found", { status: 404 });
    },
};
