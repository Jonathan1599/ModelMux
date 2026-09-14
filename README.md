# ModelMux

ModelMux is a provider-agnostic LLM inference gateway built with Node.js, TypeScript, and Fastify. It currently authenticates clients with API keys, applies a distributed per-key token bucket, and forwards non-streaming chat requests to Ollama.

## Architecture

```text
Client
  -> POST /v1/chat
  -> Bearer API key authentication
       -> SHA-256 key hash lookup in Postgres
       -> per-key capacity and refill policy
  -> atomic Redis token bucket
  -> Fastify schema validation
  -> LLMProvider interface
  -> OllamaProvider
  -> Ollama POST /api/chat (stream: false)
  -> provider-neutral ChatResponse
```

`GET /health` is public and only reports that the gateway process is running. It does not check Postgres, Redis, or Ollama.

API keys are generated with 256 bits of randomness. Only their SHA-256 hashes and short display prefixes are stored in Postgres. The full key is printed once when it is created.

Rate-limit policies are stored per API key:

- `capacity` controls the maximum burst size.
- `refill-per-second` controls the sustained request rate.
- Redis evaluates each request atomically using its own server clock.

The gateway fails closed when the API-key store or rate limiter is unavailable. Authentication and rate limiting apply only to `/v1/chat`.

## Run with Docker Compose

Build and start the gateway, Postgres, Redis, and Ollama:

```bash
docker compose up -d --build
```

Pull the model into Ollama's persistent volume:

```bash
docker compose exec ollama ollama pull llama3.2
```

Run the idempotent database migration, then create an API key with a burst capacity of 10 requests and a refill rate of 1 request per second:

```bash
docker compose exec gateway node dist/scripts/migrate.js
docker compose exec gateway node dist/scripts/create-api-key.js local-dev 10 1
```

Save the `mm_...` key printed by the second command. It cannot be recovered from the database later.

Postgres also runs SQL from `db/migrations` automatically when its data volume is first created. The explicit migration command makes setup work with an existing volume as well.

## Call the API

Replace the value below with the key returned by the creation command:

```bash
API_KEY='mm_your_key_here'

curl --request POST http://localhost:3000/v1/chat \
  --header "authorization: Bearer ${API_KEY}" \
  --header 'content-type: application/json' \
  --data '{
    "model": "llama3.2",
    "messages": [
      {
        "role": "user",
        "content": "Explain TCP in one sentence."
      }
    ]
  }'
```

A successful response has a provider-neutral shape:

```json
{
  "message": {
    "role": "assistant",
    "content": "..."
  }
}
```

Rate-limited requests return `429` and include `Retry-After`, `X-RateLimit-Limit`, and `X-RateLimit-Remaining` headers.

## Run the gateway on the host

Node.js 20 or newer is required. If Postgres is already installed locally, start only Redis and Ollama with Docker, then point `DATABASE_URL` in `.env` at the existing Postgres instance:

```bash
docker compose up -d redis ollama
docker compose exec ollama ollama pull llama3.2
npm install
cp .env.example .env
# Edit DATABASE_URL in .env if your local credentials differ from the default.
npm run db:migrate
npm run api-key:create -- local-dev 10 1
npm run dev
```

The final three arguments to `api-key:create` are the key name, burst capacity, and tokens refilled per second.

## Configuration

| Variable | Local default | Purpose |
| --- | --- | --- |
| `PORT` | `3000` | Gateway listen port |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_REQUEST_TIMEOUT_MS` | `120000` | Maximum non-streaming Ollama request duration |
| `DATABASE_URL` | `postgresql://modelmux:modelmux@localhost:5432/modelmux` | API-key metadata store |
| `REDIS_URL` | `redis://localhost:6379` | Shared token-bucket state |

Values from a local `.env` file are loaded when present.

## HTTP errors

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `VALIDATION_ERROR` | The request body failed schema validation |
| `401` | `INVALID_API_KEY` | The bearer API key is missing, malformed, disabled, or unknown |
| `429` | `RATE_LIMIT_EXCEEDED` | The API key has exhausted its token bucket |
| `502` | `PROVIDER_ERROR` | Ollama returned an unsuccessful response |
| `502` | `INVALID_PROVIDER_RESPONSE` | Ollama returned a malformed successful response |
| `503` | `API_KEY_STORE_UNAVAILABLE` | Postgres could not authenticate the request |
| `503` | `RATE_LIMITER_UNAVAILABLE` | Redis could not evaluate the request |
| `503` | `PROVIDER_UNAVAILABLE` | The gateway could not connect to Ollama |
| `504` | `PROVIDER_TIMEOUT` | Ollama exceeded its configured timeout |

Error responses include the request ID. Upstream response details remain in structured logs and are not returned to clients.

## Development commands

```bash
npm test
npm run typecheck
npm run build
npm run check
```

## Project structure

```text
.
├── db/migrations              # Idempotent Postgres schema
├── src
│   ├── app.ts                 # Fastify app factory and error handling
│   ├── config.ts              # Environment configuration
│   ├── errors.ts              # Stable application errors
│   ├── server.ts              # Runtime composition and listener
│   ├── auth
│   │   ├── api-keys.ts        # Key hashing and authentication contract
│   │   ├── guard.ts           # Protected-route authentication and limiting
│   │   └── postgres-api-key-store.ts
│   ├── providers
│   │   ├── ollama.ts          # Ollama HTTP adapter
│   │   └── provider.ts        # Provider interface
│   ├── rate-limit
│   │   ├── rate-limiter.ts    # Provider-neutral limiter contract
│   │   └── redis-token-bucket.ts
│   ├── routes/chat.ts         # Chat route and request schema
│   ├── scripts               # Migration and API-key commands
│   └── types/chat.ts          # Chat request/response types
├── test                       # Injection and unit tests
├── Dockerfile
├── docker-compose.yml
├── package.json
└── .env.example
```

Queues, BullMQ workers, caching, concurrency limiting, provider failover, circuit breakers, streaming, Prometheus, Grafana, pgvector, and load testing remain intentionally deferred to later milestones.
