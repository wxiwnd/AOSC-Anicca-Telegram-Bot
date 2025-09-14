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
const MAX_PER_RUN = 15;
const SLEEP_BETWEEN_MS = 250;

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

        lines.push(`• <b>${name}</b> <code>${before}</code> → <code>${after}</code>`);
        if (path) lines.push(`<code>${path}</code>`);
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

async function saveSigSet(env: Env, set: Set<string>): Promise<number> {
    const arr = Array.from(set);
    await env.STATE.put(KV_LAST_SIGS, JSON.stringify(arr));
    return arr.length;
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

        const prevSet = await loadSigSet(env);
        const makeSig = (u: any) => `${u.name ?? ''}|${u.after ?? ''}|${u.path ?? ''}`;


        const fresh = force ? items.slice() : items.filter((u) => !prevSet.has(makeSig(u)));
        const batch = fresh.slice(0, MAX_PER_RUN);

    if (!force && batch.length === 0) {
            await env.STATE.put(KV_LAST_HASH, newHash);
            await env.STATE.put(KV_LAST_TS, String(Date.now()));
            const ts = new Date().toISOString();
            const msg = `<b>${htmlEscape(ts)}</b>\nTotal updates: <code>${items.length}</code>\n<i>Nothing New Happened</i>`;
            await sendTelegram(env, msg);
            return { changed: false, count: items.length, hash: newHash };
        }

        // INIT record removed

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
            const header = `<b>${htmlEscape(tsHdr)}</b>\nTotal updates: <code>${items.length}</code>\nSent this run: <code>${batch.length}</code>\nRemaining: <code>${remaining}</code>`;
            await sendTelegram(env, header);
            for (let i = 0; i < msgs.length; i++) {
                try {
                    await sendTelegram(env, msgs[i]);
                    // Only record sig after success
                    prevSet.add(makeSig(batch[i]));
                    await saveSigSet(env, prevSet);
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

                // Ignore non-admin
                if (!adminId || !msg || chatType !== "private" || chatId !== String(adminId)) {
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }

                const help = "可用命令:\n"
                    + "/help - 顯示此說明\n"
                    + "/debug - 顯示狀態\n"
                    + "/send &lt;text&gt; - 發送一則訊息到預設頻道\n"
                    + "/trigger - 立即觸發抓取推送\n"
                    + "/force - 立即強制全量推送";

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
                    await safeSendTelegram(env, "已發送。", chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/trigger") {
                    ctx.waitUntil(processAndNotify(env));
                    await safeSendTelegram(env, "已排程執行", chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                if (command === "/force") {
                    ctx.waitUntil(processAndNotify(env, true));
                    await safeSendTelegram(env, "已排程強制執行", chatId);
                    return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
                }
                await safeSendTelegram(env, "未知命令，輸入 /help 查看用法", chatId);
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
