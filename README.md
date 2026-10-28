# rout.my Gemini Bridge (Render)

A small OpenAI-compatible proxy for JanitorAI that forwards requests to rout.my, keeps SSE streaming, and rewrites `max_tokens` to `max_completion_tokens` for the Gemini route.

## Important caching limitation

This proxy does **not** pretend to cache tokens locally. A local cache of prompt text does not reduce provider billing if the full conversation is still sent upstream. rout.my's docs say it adds cache markers for Claude and `qwen/qwen3.7-max`; other models must use provider-native caching and report cached token usage. For Gemini through rout.my, this bridge can preserve a stable request and help diagnose usage, but it cannot create Google's `cachedContents` on rout.my's behalf if rout.my ignores that field.

## Deploy to Render

Create a **Web Service** from a GitHub repo containing these files.

- Runtime: Node
- Build command: `npm install` (no dependencies are required; `npm install` is harmless)
- Start command: `npm start`

Set these environment variables in Render:

- `ROUTMY_API_KEY` — your rout.my API key (`sk_...`); keep secret.
- `PROXY_ACCESS_KEY` — a separate strong random password used by JanitorAI to authenticate to this proxy.
- `UPSTREAM_BASE_URL` — optional; defaults to `https://api.rout.my`.
- `INCLUDE_STREAM_USAGE` — optional; set `true` temporarily if you want the proxy to request usage metadata in streaming responses. Default `false`.

Never put API keys in source code or commit them to GitHub.

## JanitorAI settings

- API/base URL: `https://YOUR-SERVICE.onrender.com/v1`
- API key: the value of `PROXY_ACCESS_KEY`
- Model: `google/gemini-3.8-flash:flex` only if that exact ID is present in your rout.my catalog. Usually the service tier is selected separately and the model ID may be `google/gemini-3.8-flash`; use the exact ID rout.my shows for your account.

## Test

Health check:
`GET https://YOUR-SERVICE.onrender.com/health`

Models:
`GET https://YOUR-SERVICE.onrender.com/v1/models` with `Authorization: Bearer YOUR_PROXY_ACCESS_KEY`

Chat:
`POST https://YOUR-SERVICE.onrender.com/v1/chat/completions` with the same Bearer header and a normal OpenAI Chat Completions JSON body.

## What the token rewrite does

If JanitorAI sends:
`{"max_tokens": 300}`

the bridge sends rout.my:
`{"max_completion_tokens": 300}`

If both are present, an explicit positive integer `max_completion_tokens` wins and `max_tokens` is removed.

This only fixes the field name. It cannot force an upstream model/provider to obey a cap if the upstream route ignores the field. Test with a low limit and inspect the actual output.

## Security and privacy

- The proxy requires `PROXY_ACCESS_KEY`; without it, the server refuses to start.
- Prompts and responses are not logged.
- Do not share the Render service URL plus the proxy key.
- Render free instances may sleep, so the first request after inactivity may be slow.
