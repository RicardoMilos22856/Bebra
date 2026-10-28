import http from "node:http";
import { URL } from "node:url";
import { timingSafeEqual } from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const ROUTMY_API_KEY = process.env.ROUTMY_API_KEY;
const PROXY_ACCESS_KEY = process.env.PROXY_ACCESS_KEY;
const UPSTREAM_BASE_URL = (process.env.UPSTREAM_BASE_URL || "https://api.rout.my/v1").replace(/\/+$/, "");
const INCLUDE_STREAM_USAGE = String(process.env.INCLUDE_STREAM_USAGE || "false").toLowerCase() === "true";
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES || 20 * 1024 * 1024);

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
  // Constant-time comparison for equal-length buffers.
  const a = Buffer.from(supplied);
  const b = Buffer.from(PROXY_ACCESS_KEY);
  if (a.length !== b.length) return false;
  return cryptoTimingSafeEqual(a, b);
}

function cryptoTimingSafeEqual(a, b) {
  return timingSafeEqual(a, b);
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

function cleanUpstreamHeaders(upstreamHeaders, res) {
  // Do not forward hop-by-hop or potentially stale content-length/encoding headers.
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
  ];
  for (const name of allowed) {
    const value = upstreamHeaders.get(name);
    if (value) res.setHeader(name, value);
  }
  res.setHeader("x-proxy-name", "routmy-gemini-bridge");
  res.setHeader("cache-control", "no-store");
}

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
    console.error("Upstream response stream failed:", error?.message || "unknown error");
    if (!res.headersSent) {
      json(res, 502, { error: { message: "Upstream stream failed", type: "proxy_error" } });
    } else {
      res.destroy(error);
    }
  }
}

const server = http.createServer(async (req, res) => {
  const requestUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const pathname = requestUrl.pathname;

  if (req.method === "GET" && pathname === "/health") {
    return json(res, 200, { ok: true, service: "routmy-gemini-bridge" });
  }

  if (!isAuthorized(req)) {
    return json(res, 401, {
      error: { message: "Unauthorized. Use the proxy access key as a Bearer token.", type: "authentication_error" },
    });
  }

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

  if (req.method !== "POST" || !allowedPaths.has(pathname) || pathname !== "/v1/chat/completions") {
    return json(res, 404, { error: { message: "Endpoint not found", type: "invalid_request_error" } });
  }

  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    return json(res, error.status || 400, {
      error: { message: error.message || "Invalid request", type: "invalid_request_error" },
    });
  }

  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return json(res, 400, { error: { message: "JSON body must be an object", type: "invalid_request_error" } });
  }

  // JanitorAI typically sends max_tokens. rout.my documents max_completion_tokens
  // as the canonical output cap; support specifically advised trying this field
  // for Gemini. Preserve an explicitly supplied max_completion_tokens.
  let capSource = "none";
  if (Number.isInteger(body.max_completion_tokens) && body.max_completion_tokens > 0) {
    capSource = "max_completion_tokens";
    delete body.max_tokens;
  } else if (Number.isInteger(body.max_tokens) && body.max_tokens > 0) {
    body.max_completion_tokens = body.max_tokens;
    delete body.max_tokens;
    capSource = "max_tokens→max_completion_tokens";
  } else {
    delete body.max_tokens;
  }

  // Optional diagnostics: ask for the standard final usage chunk in SSE streams.
  // Leave off by default to avoid changing client behavior.
  if (body.stream === true && INCLUDE_STREAM_USAGE) {
    body.stream_options = { ...(body.stream_options || {}), include_usage: true };
  }

  // Do not log prompts, character cards, or API keys.
  console.log(JSON.stringify({
    event: "chat_request",
    model: typeof body.model === "string" ? body.model : "unspecified",
    stream: body.stream === true,
    output_limit_field: capSource,
  }));

  const upstreamUrl = `${UPSTREAM_BASE_URL}/v1/chat/completions`;
  let upstream;
  try {
    upstream = await fetch(upstreamUrl, {
      method: "POST",
      headers: makeUpstreamHeaders(),
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10 * 60_000),
    });
  } catch (error) {
    console.error("Upstream request failed:", error?.message || "unknown error");
    return json(res, 502, { error: { message: "Could not reach rout.my", type: "proxy_error" } });
  }

  console.log(JSON.stringify({
    event: "upstream_response",
    status: upstream.status,
    model: typeof body.model === "string" ? body.model : "unspecified",
    streaming: body.stream === true,
    request_id: upstream.headers.get("x-request-id") || undefined,
  }));

  return await forwardResponse(upstream, res);
});

server.requestTimeout = 11 * 60_000;
server.headersTimeout = 12 * 60_000;
server.keepAliveTimeout = 65_000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`rout.my Gemini bridge listening on port ${PORT}`);
});
