import http from "node:http";
import { URL } from "node:url";
import { timingSafeEqual } from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const ROUTMY_API_KEY = process.env.ROUTMY_API_KEY;
const PROXY_ACCESS_KEY = process.env.PROXY_ACCESS_KEY;
const UPSTREAM_BASE_URL = (process.env.UPSTREAM_BASE_URL || "https://api.rout.my/v1").replace(/\/+$/, "");
const INCLUDE_STREAM_USAGE = String(process.env.INCLUDE_STREAM_USAGE || "false").toLowerCase() === "true";
const ENABLE_CACHING = String(process.env.ENABLE_CACHING || "true").toLowerCase() === "true";
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES || 20 * 1024 * 1024);

// Для совместимости с Janitor AI
const JANITOR_COMPATIBILITY_MODE = String(process.env.JANITOR_COMPATIBILITY_MODE || "true").toLowerCase() === "true";

if (!ROUTMY_API_KEY) {
  console.error("Missing ROUTMY_API_KEY environment variable.");
  process.exit(1);
}

if (!PROXY_ACCESS_KEY) {
  console.error("Missing PROXY_ACCESS_KEY. Set a private key so this server cannot be used as an open proxy.");
  process.exit(1);
}

const allowedPaths = new Set(["/v1/chat/completions", "/v1/models"]);

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
  });
  res.end(body);
}

function isAuthorized(req) {
  const auth = req.headers.authorization || "";
  const supplied = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  const a = Buffer.from(supplied);
  const b = Buffer.from(PROXY_ACCESS_KEY);
  if (a.length !== b.length) return false;
  try {
    return timingSafeEqual(a, b);
  } catch {
    return false;
  }
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
  const raw = Buffer.concat(chunks).toString("utf8");
  try {
    return JSON.parse(raw);
  } catch {
    const error = new Error("Request body must be valid JSON");
    error.status = 400;
    throw error;
  }
}

function makeUpstreamHeaders() {
  return {
    authorization: `Bearer ${ROUTMY_API_KEY}`,
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
  };
}

function isGeminiModel(model) {
  return typeof model === "string" && model.toLowerCase().includes("gemini");
}

function isClaudeModel(model) {
  return typeof model === "string" && model.toLowerCase().includes("claude");
}

/**
 * Нормализует названия моделей для rout.my
 * Janitor AI может отправлять названия в разных форматах
 */
function normalizeModelName(model) {
  if (!model || typeof model !== "string") return model;
  
  // Преобразования для Gemini
  if (model.includes("gemini")) {
    // Если уже имеет префикс google/, пропускаем
    if (model.startsWith("google/")) return model;
    
    // google/gemini-2.0-flash, google/gemini-1.5-pro и т.д.
    return `google/${model}`;
  }

  // Преобразования для Claude
  if (model.includes("claude")) {
    if (model.startsWith("anthropic/")) return model;
    return `anthropic/${model}`;
  }

  return model;
}

/**
 * Добавляет кеширование в зависимости от модели
 */
function addCachingSupport(body) {
  if (!ENABLE_CACHING) return;

  const model = body.model;

  // Для Claude моделей используем smart_cache
  if (isClaudeModel(model)) {
    if (!body.smart_cache) {
      body.smart_cache = true;
    }
  }

  // Для Gemini моделей используем cache_control в системном промпте
  if (isGeminiModel(model)) {
    // Обрабатываем messages для добавления кеширования
    if (Array.isArray(body.messages) && body.messages.length > 0) {
      // Добавляем кеш к системному сообщению или первому сообщению
      for (let i = 0; i < body.messages.length; i++) {
        const msg = body.messages[i];
        
        // Если это системное сообщение
        if (msg.role === "system" && typeof msg.content === "string") {
          // Преобразуем в структурированный формат с кешем
          body.messages[i] = {
            role: "system",
            content: [
              {
                type: "text",
                text: msg.content,
                cache_control: { type: "ephemeral" }
              }
            ]
          };
          break;
        }
        
        // Если это первое пользовательское сообщение в начале
        if (msg.role === "user" && i < 2 && typeof msg.content === "string") {
          // Кешируем контекст в первом пользовательском сообщении
          body.messages[i] = {
            role: "user",
            content: [
              {
                type: "text",
                text: msg.content,
                cache_control: { type: "ephemeral" }
              }
            ]
          };
          break;
        }
      }
    }
  }
}

function cleanUpstreamHeaders(upstreamHeaders, res) {
  const allowed = [
    "content-type",
    "cache-control",
    "x-request-id",
    "retry-after",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "x-context-limit",
    "x-context-original-tokens",
    // Для Gemini кеширования
    "x-cache-used-input-tokens",
    "x-cache-creation-input-tokens",
  ];
  
  for (const name of allowed) {
    const value = upstreamHeaders.get(name);
    if (value) res.setHeader(name, value);
  }
  
  res.setHeader("x-proxy-name", "janitor-gemini-bridge");
  res.setHeader("cache-control", "no-store");
}

/**
 * Обрабатывает SSE поток (для streaming responses)
 */
async function forwardStreamResponse(upstream, res) {
  res.statusCode = upstream.status;
  cleanUpstreamHeaders(upstream.headers, res);
  
  if (!upstream.body) {
    res.end();
    return;
  }

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      
      // Оставляем последнюю неполную строку в буфере
      buffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim()) {
          // Пустая строка - разделитель событий
          res.write("\n");
          continue;
        }

        // Передаем SSE данные как есть
        res.write(line + "\n");
      }
    }

    // Обрабатываем оставшиеся данные в буфере
    if (buffer.trim()) {
      res.write(buffer + "\n");
    }

    res.end();
  } catch (error) {
    console.error("Stream error:", error?.message || "unknown error");
    if (!res.headersSent) {
      json(res, 502, { error: { message: "Stream processing failed", type: "proxy_error" } });
    } else {
      res.destroy(error);
    }
  }
}

/**
 * Обрабатывает обычный ответ (non-streaming)
 */
async function forwardResponse(upstream, res) {
  res.statusCode = upstream.status;
  cleanUpstreamHeaders(upstream.headers, res);

  if (!upstream.body) {
    res.end();
    return;
  }

  const reader = upstream.body.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!res.write(Buffer.from(value))) {
        await new Promise((resolve) => res.once("drain", resolve));
      }
    }
    res.end();
  } catch (error) {
    console.error("Response stream failed:", error?.message || "unknown error");
    if (!res.headersSent) {
      json(res, 502, { error: { message: "Response failed", type: "proxy_error" } });
    } else {
      res.destroy(error);
    }
  }
}

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const pathname = requestUrl.pathname;

  // Health check для Janitor AI мониторинга
  if (req.method === "GET" && (pathname === "/health" || pathname === "/")) {
    return json(res, 200, { 
      ok: true, 
      service: "janitor-gemini-bridge",
      janitor_mode: JANITOR_COMPATIBILITY_MODE,
      caching_enabled: ENABLE_CACHING
    });
  }

  if (!isAuthorized(req)) {
    return json(res, 401, {
      error: { message: "Unauthorized. Use the proxy access key as a Bearer token.", type: "authentication_error" },
    });
  }

  // Список доступных моделей
  if (req.method === "GET" && pathname === "/v1/models") {
    try {
      const upstream = await fetch(`${UPSTREAM_BASE_URL}/v1/models`, {
        method: "GET",
        headers: { authorization: `Bearer ${ROUTMY_API_KEY}`, accept: "application/json" },
        signal: AbortSignal.timeout(60_000),
      });
      return await forwardResponse(upstream, res);
    } catch (error) {
      console.error("Models request failed:", error?.message || "unknown error");
      return json(res, 502, { error: { message: "Could not reach rout.my", type: "proxy_error" } });
    }
  }

  // Обработка chat completions
  if (req.method === "POST" && pathname === "/v1/chat/completions") {
    let body;
    try {
      body = await readJsonBody(req);
    } catch (error) {
      return json(res, error.status || 400, {
        error: { message: error.message || "Invalid request", type: "invalid_request_error" },
      });
    }

    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return json(res, 400, {
        error: { message: "JSON body must be an object", type: "invalid_request_error" },
      });
    }

    // Нормализуем название модели
    if (body.model) {
      body.model = normalizeModelName(body.model);
    }

    // Обработка max_tokens / max_completion_tokens
    if (Number.isInteger(body.max_completion_tokens) && body.max_completion_tokens > 0) {
      delete body.max_tokens;
    } else if (Number.isInteger(body.max_tokens) && body.max_tokens > 0) {
      body.max_completion_tokens = body.max_tokens;
      delete body.max_tokens;
    } else {
      delete body.max_tokens;
      delete body.max_completion_tokens;
    }

    // Добавляем поддержку кеширования
    addCachingSupport(body);

    // Включаем usage в потоке если нужно
    if (body.stream === true && INCLUDE_STREAM_USAGE) {
      body.stream_options = { ...(body.stream_options || {}), include_usage: true };
    }

    const isGemini = isGeminiModel(body.model);
    const isClaude = isClaudeModel(body.model);

    console.log(JSON.stringify({
      event: "chat_request",
      timestamp: new Date().toISOString(),
      model: body.model || "unspecified",
      model_type: isGemini ? "gemini" : isClaude ? "claude" : "other",
      stream: body.stream === true,
      caching_enabled: ENABLE_CACHING,
      janitor_mode: JANITOR_COMPATIBILITY_MODE,
    }));

    const upstreamUrl = `${UPSTREAM_BASE_URL}/v1/chat/completions`;
    
    try {
      const upstream = await fetch(upstreamUrl, {
        method: "POST",
        headers: makeUpstreamHeaders(),
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(10 * 60_000),
      });

      console.log(JSON.stringify({
        event: "upstream_response",
        timestamp: new Date().toISOString(),
        status: upstream.status,
        model: body.model || "unspecified",
        streaming: body.stream === true,
        request_id: upstream.headers.get("x-request-id") || "unknown",
      }));

      // Выбираем обработчик в зависимости от типа ответа
      if (body.stream === true) {
        return await forwardStreamResponse(upstream, res);
      } else {
        return await forwardResponse(upstream, res);
      }
    } catch (error) {
      console.error("Upstream request failed:", error?.message || "unknown error");
      return json(res, 502, {
        error: { message: "Could not reach rout.my", type: "proxy_error" },
      });
    }
  }

  // 404 для неизвестных путей
  return json(res, 404, {
    error: { message: "Endpoint not found", type: "invalid_request_error" },
  });
});

server.requestTimeout = 11 * 60_000;
server.headersTimeout = 12 * 60_000;
server.keepAliveTimeout = 65_000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`
╔═══════════════════════════════════════╗
║   Janitor AI → Gemini Bridge Server   ║
╚═══════════════════════════════════════╝

Server listening on port ${PORT}
- Janitor Compatibility Mode: ${JANITOR_COMPATIBILITY_MODE}
- Caching Enabled: ${ENABLE_CACHING}
- API Base: ${UPSTREAM_BASE_URL}

Setup:
1. In Janitor AI, set custom endpoint to: http://your-server:${PORT}/v1/
2. Use this Bearer token in Janitor AI settings
3. Model names will be auto-normalized for rout.my
