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
const MAX_PER_RUN = 15;
const SLEEP_BETWEEN_MS = 250;

type Subscriber = { id: string; name: string };

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

function renderUpdateLines(updates: any[]): string[] {
    // updates: array of { name, before, after, path, warnings? }
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
        // Links: PackageInfo | Changelog (based on package name)
        const nameForUrl = String(u.name ?? "").trim();
        if (nameForUrl) {
            const encoded = encodeURIComponent(nameForUrl);
            const pkgUrl = `https://packages.aosc.io/packages/${encoded}`;
            const changelogUrl = `https://packages.aosc.io/changelog/${encoded}`;
            lines.push(`links: <a href="${pkgUrl}">PackageInfo</a> | <a href="${changelogUrl}">Changelog</a>`);
        }
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

async function loadSigSet(env: Env): Promise<Set<string>> {
    const raw = (await env.STATE.get(KV_LAST_SIGS)) || "";
    if (!raw) return new Set();
    const trimmed = raw.trim();
    if (trimmed.startsWith("[")) {
        try {
            const arr = JSON.parse(trimmed);
            if (Array.isArray(arr)) return new Set(arr.filter((x) => typeof x === "string" && x.length > 0));
        } catch {}
        return new Set();
    }
    return new Set(trimmed.split("\n").filter(Boolean));
}

// Ordered signature store utilities to preserve recency and trim efficiently
async function loadSigStore(env: Env): Promise<{ list: string[]; set: Set<string> }> {
    const raw = (await env.STATE.get(KV_LAST_SIGS)) || "";
    let list: string[] = [];
    if (raw) {
        const trimmed = raw.trim();
        if (trimmed.startsWith("[")) {
            try {
                const arr = JSON.parse(trimmed);
                if (Array.isArray(arr)) list = arr.filter((x) => typeof x === "string" && x.length > 0);
            } catch {}
        } else {
            list = trimmed.split("\n").filter(Boolean);
        }
    }
    return { list, set: new Set(list) };
}

async function pushSigAndSave(env: Env, store: { list: string[]; set: Set<string> }, sig: string): Promise<number> {
    const { list, set } = store;
    // If exists, move to tail to mark recent
    const idx = list.indexOf(sig);
    if (idx >= 0) {
        list.splice(idx, 1);
        list.push(sig);
    } else {
        list.push(sig);
        set.add(sig);
    }
     // Persist
     const toSave = store.list || list;
     await env.STATE.put(KV_LAST_SIGS, JSON.stringify(toSave));
     return toSave.length;
}

// Keep only signatures that are still present in the current dataset
async function reconcileSigStore(env: Env, store: { list: string[]; set: Set<string> }, allowed: Set<string>): Promise<number> {
    const filtered = (store.list || []).filter((s) => allowed.has(s));
    store.list = filtered;
    store.set = new Set(filtered);
    await env.STATE.put(KV_LAST_SIGS, JSON.stringify(filtered));
    return filtered.length;
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

async function addUserSubscription(env: Env, userId: string, pkgLower: string): Promise<{ added: boolean; already: boolean; size: number } > {
    const data = await loadUserSubscriptions(env, userId);
    const already = !!data[pkgLower];
    if (already) {
        return { added: false, already: true, size: Object.keys(data).length };
    }
    data[pkgLower] = true;
    await saveUserSubscriptions(env, userId, data);
    return { added: true, already: false, size: Object.keys(data).length };
}

// Collect subscribers for a package across all users
async function getSubscribersForPackageAll(env: Env, pkgLower: string): Promise<Subscriber[]> {
    const userIds = await listSubscriptionUserIds(env);
    if (userIds.length === 0) return [];
    const result: Subscriber[] = [];
    for (const uid of userIds) {
        try {
            const subsMap = await loadUserSubscriptions(env, uid);
            if (subsMap[pkgLower]) {
                result.push({ id: String(uid), name: "" });
            }
        } catch (e) {
            await recordError(env, "subsCollect", e);
        }
    }
    return result;
}

async function removeUserSubscription(env: Env, userId: string, pkgLower: string): Promise<{ removed: boolean; size: number } > {
    const data = await loadUserSubscriptions(env, userId);
    const existed = !!data[pkgLower];
    if (existed) delete data[pkgLower];
    await saveUserSubscriptions(env, userId, data);
    return { removed: existed, size: Object.keys(data).length };
}

async function getAdminId(env: Env): Promise<string | null> {
    const fromEnv = (env.ADMIN_CHAT_ID || "").trim();
    if (fromEnv) return fromEnv;
    const fromKV = await env.STATE.get(KV_ADMIN_CHAT_ID);
    return fromKV || null;
}

async function getConfig(env: Env): Promise<{ token: string | null; chatId: string | null; secret: string | null }> {
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
    if (!res.ok) {
        const body = await res.text();
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

async function fetchUpdates(env: Env): Promise<{ raw: string; items: any[] } | null> {
    const url = env.FETCH_URL || DEFAULT_URL;
    const res = await fetch(url);
    if (!res.ok) return null;
    const raw = await res.text();
    try {
        const items = JSON.parse(raw);
        if (!Array.isArray(items)) return null;
        return { raw, items };
    } catch {
        return null;
    }
}

async function processAndNotify(env: Env, force = false): Promise<{ changed: boolean; count: number; hash?: string; messages?: string[] }> {
    try {
        const data = await fetchUpdates(env);
        if (!data) return { changed: false, count: 0 };
        const { raw, items } = data;
        const newHash = hashString(raw);

        const sigStore = await loadSigStore(env);
        let prevSet = sigStore.set;
        const makeSig = (u: any) => `${u.name ?? ''}|${u.after ?? ''}|${u.path ?? ''}`;


        const fresh = force ? items.slice() : items.filter((u) => !prevSet.has(makeSig(u)));
        const batch = fresh.slice(0, MAX_PER_RUN);
        const allowedSigs = new Set(items.map((u) => makeSig(u)));

    if (!force && batch.length === 0) {
            // Prune signatures not in current dataset to avoid unbounded growth
            await reconcileSigStore(env, sigStore, allowedSigs);
             await env.STATE.put(KV_LAST_HASH, newHash);
             await env.STATE.put(KV_LAST_TS, String(Date.now()));
             const ts = new Date().toISOString();
             const msg = `<b>${htmlEscape(ts)}</b>\nTotal updates: <code>${items.length}</code>\n<i>Nothing New Happened</i>`;
             await sendTelegram(env, msg);
             return { changed: false, count: items.length, hash: newHash };
         }

        // Update STATE
        await env.STATE.put(KV_LAST_HASH, newHash);
        await env.STATE.put(KV_LAST_TS, String(Date.now()));

        const msgs = batch.map((u) => renderUpdateLines([u]).join("\n"));
        if (msgs.length === 0) {
            await sendTelegram(env, "Nothing but void.");
        } else {
            // Send a summary header with time and TOTAL updates count
            const tsHdr = new Date().toISOString();
            const remaining = Math.max(0, fresh.length - batch.length);
            const header = `<b>${htmlEscape(tsHdr)}</b>\nSummary:\nTotal: <code>${items.length}</code>\nSent this run: <code>${batch.length}</code>\nRemaining: <code>${remaining}</code>`;
            await sendTelegram(env, header);
            for (let i = 0; i < msgs.length; i++) {
                try {
                    await sendTelegram(env, msgs[i]);
                    // Only record sig after success; will be pruned against current dataset later
                    const sig = makeSig(batch[i]);
                    await pushSigAndSave(env, sigStore, sig);
                    // refresh local set reference in case of trimming
                    prevSet = sigStore.set;

                    // Subscription notifications via private chat (DM) to subscribers
                    const pkgName = String(batch[i]?.name || "");
                    const pkgLower = pkgName.toLowerCase();
                    if (pkgLower) {
                        const rendered = renderUpdateLines([batch[i]]).join("\n");
                        const subs = await getSubscribersForPackageAll(env, pkgLower);
                        for (const s of subs) {
                            try {
                                await sendTelegram(env, rendered, String(s.id));
                            } catch (e) {
                                await recordError(env, "subsNotifyDM", e);
                            }
                        }
                    }
                } catch (e) {
                    await recordError(env, "sendUpdate", e);
                    await notifyAdminError(env, "sendUpdate", e);
                    // Send error msg to admin
                    try {
                        const adminId = await getAdminId(env);
                        if (adminId) {
                            const tsNow = new Date().toISOString();
                            const sentSoFar = i;
                            const remaining = Math.max(0, fresh.length - sentSoFar);
                            const note = `<b>本輪提前結束</b> @ <b>${htmlEscape(tsNow)}</b>\nSent this run: <code>${sentSoFar}</code>\nRemaining: <code>${remaining}</code>`;
                            await safeSendTelegram(env, note, adminId);
                        }
                    } catch (e2) {
                        console.error("notify admin early-termination failed", e2);
                    }
                    break;
                }
                if (i < msgs.length - 1 && SLEEP_BETWEEN_MS > 0) await sleep(SLEEP_BETWEEN_MS);
            }
            // After sending, prune any old signatures not in current dataset
            await reconcileSigStore(env, sigStore, allowedSigs);
         }

        return { changed: true, count: items.length, hash: newHash, messages: force ? msgs : undefined };
    } catch (e) {
        await recordError(env, "processAndNotify", e);
        await notifyAdminError(env, "processAndNotify", e);
        return { changed: false, count: 0 };
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

const PUBLIC_COMMANDS = new Set<string>(["/new_member", "/findupd", "/subscribe", "/unsubscribe", "/listsub", "/help"]);

export default {
    async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
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
        const url = new URL(request.url);
        if (url.pathname === "/") {
            const lastHash = await env.STATE.get(KV_LAST_HASH);
            const lastTs = await env.STATE.get(KV_LAST_TS);
            return new Response(JSON.stringify({ ok: true, lastHash, lastTs, source: env.FETCH_URL || DEFAULT_URL }), { headers: { "content-type": "application/json" } });
        }

        if (url.pathname === "/trigger") {
            if (!(await auth(request, env))) return new Response("Unauthorized", { status: 401 });
            const force = url.searchParams.get("force") === "1";
            const sync = url.searchParams.get("sync") === "1";
            try {
                if (sync) {
                    const res = await processAndNotify(env, force);
                    return new Response(JSON.stringify({ ok: true, mode: "sync", ...res }, null, 2), { headers: { "content-type": "application/json" } });
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
                    + "/force - 立即强制全量推送(admin)\n"
                    + "/findupd &lt;package-name&gt; - 查找可能存在的上游更新\n"
                    + "/subscribe &lt;package-name[;package2;...]&gt; - 订阅指定软件包更新\n"
                    + "/unsubscribe &lt;package-name&gt; - 取消订阅指定软件包\n"
                    + "/listsub [package-name] - 列出我的订阅";

                if (command === "/start" || command === "/help") {
                    await safeSendTelegram(env, help, chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/debug") {
                    const lastHash = await env.STATE.get(KV_LAST_HASH);
                    const lastTs = await env.STATE.get(KV_LAST_TS);
                    const sigSet = await loadSigSet(env);
                    const sigCount = sigSet.size;
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
                if (command === "/force") {
                    ctx.waitUntil(processAndNotify(env, true));
                    await safeSendTelegram(env, "已排程强制执行", chatId);
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
                    // Validate existence against current dataset
                    const updates = await fetchUpdates(env);
                    if (!updates) {
                        await safeSendTelegram(env, "出现问题，请稍后再试", chatId);
                        return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                    }
                    const nameSet = new Set<string>((updates.items || []).map((it: any) => String(it.name || "").toLowerCase()).filter(Boolean));
                    const valids: string[] = [];
                    const invalids: string[] = [];
                    for (const pl of uniqueLowers) {
                        if (nameSet.has(pl)) valids.push(pl); else invalids.push(pl);
                    }
                    const userId = String(msg?.from?.id || "");
                    let added = 0, already = 0;
                    for (const v of valids) {
                        try {
                            const res = await addUserSubscription(env, userId, v);
                            if (res.added) added++; else if (res.already) already++;
                        } catch (e) {
                            await recordError(env, "subscribe.add", e);
                        }
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
