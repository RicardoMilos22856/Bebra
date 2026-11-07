import http from "node:http";
import { URL } from "node:url";

// ==================== КОНФИГУРАЦИЯ ====================
const PORT = Number.parseInt(process.env.PORT || "3000", 10);

const ROUTMY_API_KEY = process.env.ROUTMY_API_KEY;
const UPSTREAM_BASE_URL = (
  process.env.UPSTREAM_BASE_URL || "https://api.rout.my"
).replace(/\/+$/, "");

const INCLUDE_STREAM_USAGE =
  String(process.env.INCLUDE_STREAM_USAGE || "false").toLowerCase() === "true";

const ENABLE_CACHING =
  String(process.env.ENABLE_CACHING || "true").toLowerCase() === "true";

const MAX_BODY_BYTES = Number(
  process.env.MAX_BODY_BYTES || 20 * 1024 * 1024
);

// ==================== ВАЛИДАЦИЯ ====================
if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
  console.error("Invalid PORT");
  process.exit(1);
}

if (!ROUTMY_API_KEY) {
  console.error("Missing ROUTMY_API_KEY environment variable.");
  process.exit(1);
}

console.log("Starting Janitor AI → Gemini Bridge");
console.log("PORT:", PORT);
console.log("UPSTREAM_BASE_URL:", UPSTREAM_BASE_URL);
console.log("ENABLE_CACHING:", ENABLE_CACHING);
console.log("INCLUDE_STREAM_USAGE:", INCLUDE_STREAM_USAGE);
console.log("ROUTMY_API_KEY: configured");

// ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  res.end(body);
}

async function readJsonBody(req) {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      const error = new Error("Request body too large");
      error.status = 413;
      throw error;
    }
    chunks.push(chunk);
  }

  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    const error = new Error("Request body must be valid JSON");
    error.status = 400;
    throw error;
  }
}

function makeUpstreamHeaders() {
  return {
    Authorization: `Bearer ${ROUTMY_API_KEY}`,
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
  };
}

function isGeminiModel(model) {
  return typeof model === "string" && model.toLowerCase().includes("gemini");
}

function isClaudeModel(model) {
  return typeof model === "string" && model.toLowerCase().includes("claude");
}

function normalizeModelName(model) {
  if (typeof model !== "string" || !model) return model;

  const lower = model.toLowerCase();

  if (lower.includes("gemini") && !lower.startsWith("google/")) {
    return `google/${model}`;
  }

  if (lower.includes("claude") && !lower.startsWith("anthropic/")) {
    return `anthropic/${model}`;
  }

  return model;
}

// ==================== КЭШИНГ (только Claude) ====================
function addCachingSupport(body) {
  if (!ENABLE_CACHING) return;
  if (isClaudeModel(body.model)) {
    body.smart_cache = true;
  }
}

// ==================== ФОРВАРДИНГ ====================
function cleanUpstreamHeaders(upstream, res) {
  const allowed = [
    "content-type", "x-request-id", "retry-after", "x-ratelimit-limit",
    "x-ratelimit-remaining", "x-ratelimit-reset", "x-context-limit",
    "x-context-original-tokens", "x-cache-used-input-tokens", "x-cache-creation-input-tokens"
  ];

  for (const name of allowed) {
    const value = upstream.headers.get(name);
    if (value) res.setHeader(name, value);
  }
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Cache-Control", "no-store");
}

async function forwardResponse(upstream, res) {
  res.statusCode = upstream.status;
  cleanUpstreamHeaders(upstream, res);

  if (!upstream.body) return res.end();

  const reader = upstream.body.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!res.write(Buffer.from(value))) await new Promise(resolve => res.once("drain", resolve));
    }
    res.end();
  } catch (e) {
    console.error("Forward failed:", e.message);
    if (!res.headersSent) json(res, 502, { error: { message: "Response forwarding failed", type: "proxy_error" } });
    else res.destroy(e);
  }
}

async function forwardStreamResponse(upstream, res) {
  res.statusCode = upstream.status;
  cleanUpstreamHeaders(upstream, res);

  if (!upstream.body) return res.end();

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      if (chunk) res.write(chunk);
    }
    const remaining = decoder.decode();
    if (remaining) res.write(remaining);
    res.end();
  } catch (e) {
    console.error("Stream forward failed:", e.message);
    if (!res.headersSent) json(res, 502, { error: { message: "Stream forwarding failed", type: "proxy_error" } });
    else res.destroy(e);
  }
}

// ==================== ОСНОВНОЙ СЕРВЕР ====================
const server = http.createServer(async (req, res) => {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Expose-Headers", "x-request-id, x-proxy-name, x-cache-used-input-tokens, x-cache-creation-input-tokens");
  res.setHeader("Cache-Control", "no-store");

  // OPTIONS Preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  // Health check
  if (req.method === "GET" && (req.url === "/" || req.url === "/health")) {
    return json(res, 200, {
      ok: true,
      service: "janitor-gemini-bridge",
      caching_enabled: ENABLE_CACHING,
    });
  }

  // /v1/models
  if (req.method === "GET" && req.url === "/v1/models") {
    try {
      const upstream = await fetch(`${UPSTREAM_BASE_URL}/v1/models`, {
        method: "GET",
        headers: makeUpstreamHeaders(),
        signal: AbortSignal.timeout(60_000),
      });
      return await forwardResponse(upstream, res);
    } catch (e) {
      return json(res, 502, { error: { message: "Could not reach rout.my", type: "proxy_error" } });
    }
  }

  // /v1/chat/completions
  if (req.method === "POST" && req.url === "/v1/chat/completions") {
    console.log(`Chat request from ${req.headers["x-forwarded-for"] || req.socket.remoteAddress}`);

    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      return json(res, e.status || 400, { error: { message: e.message || "Invalid request", type: "invalid_request_error" } });
    }

    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return json(res, 400, { error: { message: "JSON body must be an object", type: "invalid_request_error" } });
    }

    if (body.model) body.model = normalizeModelName(body.model);

    // Нормализация токенов
    if (Number.isInteger(body.max_completion_tokens) && body.max_completion_tokens > 0) {
      delete body.max_tokens;
    } else if (Number.isInteger(body.max_tokens) && body.max_tokens > 0) {
      body.max_completion_tokens = body.max_tokens;
      delete body.max_tokens;
    } else {
      delete body.max_tokens;
      delete body.max_completion_tokens;
    }

    // ==================== БЕЗОПАСНЫЙ ОБХОД ЦЕНЗУРЫ ДЛЯ GEMINI ====================
    if (isGeminiModel(body.model)) {
      // Если Janitor НЕ передал свои настройки, создаем дефолтные BLOCK_NONE
      if (!body.safety_settings) {
        body.safety_settings = [
          { category: "HATE_SPEECH", threshold: "BLOCK_NONE" },
          { category: "HARASSMENT", threshold: "BLOCK_NONE" },
          { category: "SEXUALLY_EXPLICIT", threshold: "BLOCK_NONE" },
          { category: "DANGEROUS_CONTENT", threshold: "BLOCK_NONE" },
          { category: "CIVIC_INTEGRITY", threshold: "BLOCK_NONE" }
        ];
      }

      // Синхронизируем extra_body (подмешиваем safety_settings туда, если прокси требует этого формата)
      if (body.extra_body && typeof body.extra_body === 'object') {
        if (!body.extra_body.safety_settings) {
          body.extra_body.safety_settings = body.safety_settings;
        }
      } else {
        body.extra_body = { safety_settings: body.safety_settings };
      }
    }

    // Кэширование + stream_options
    addCachingSupport(body);

    if (body.stream === true && INCLUDE_STREAM_USAGE) {
      body.stream_options = { ...(body.stream_options || {}), include_usage: true };
    }

    const upstreamUrl = `${UPSTREAM_BASE_URL}/v1/chat/completions`;

    try {
      const upstream = await fetch(upstreamUrl, {
        method: "POST",
        headers: makeUpstreamHeaders(),
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(10 * 60_000),
      });

      // ГЛУБОКОЕ ЛОГИРОВАНИЕ ПРИ ОШИБКЕ 400 BAD REQUEST
      if (upstream.status === 400) {
        const errorText = await upstream.text();
        console.error("===== [!] КРИТИЧЕСКАЯ ОШИБКА 400 ОТ ROUT.MY =====");
        console.error("Ответ от rout.my:", errorText);
        console.error("Тот JSON-запрос, который мы им отправили:");
        console.error(JSON.stringify(body, null, 2));
        console.error("=================================================");
        
        res.statusCode = 400;
        res.setHeader("Content-Type", "application/json");
        return res.end(errorText);
      }

      if (body.stream === true) {
        return await forwardStreamResponse(upstream, res);
      }
      return await forwardResponse(upstream, res);
    } catch (e) {
      console.error("Исключение при запросе к апстриму:", e.message);
      return json(res, 502, { error: { message: "Could not reach rout.my", type: "proxy_error" } });
    }
  }

  return json(res, 404, { error: { message: "Endpoint not found", type: "invalid_request_error" } });
});
server.listen(PORT, "0.0.0.0", () => {
console.log(✅ Server listening on port ${PORT});
console.log(API Base: ${UPSTREAM_BASE_URL});
console.log(Caching: ${ENABLE_CACHING ? "enabled" : "disabled"});
});
