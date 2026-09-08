# ModelMux

This milestone is a small HTTP gateway that exposes a provider-neutral chat API and forwards requests to a local Ollama instance. It uses Fastify for HTTP and validation, Pino for structured logs, and native `fetch` for the Ollama call.

## Architecture

```text
Client
  -> POST /v1/chat
  -> Fastify schema validation
  -> LLMProvider interface
  -> OllamaProvider
  -> Ollama POST /api/chat (stream: false)
  -> provider-neutral ChatResponse
```

`GET /health` only reports that the gateway process is running. It does not contact Ollama in this milestone.

## Run with Docker Compose

Start Ollama, then pull the model once into its persistent Docker volume:

```bash
docker compose up -d ollama
docker compose exec ollama ollama pull llama3.2
```

Build and start the gateway:

```bash
docker compose up --build gateway
```

Ollama model download is deliberately a separate command so gateway startup is predictable and does not depend on a long model pull.

## Run directly on the host

Node.js 20 or newer and a local [Ollama](https://ollama.com/) installation are required.

In one terminal, start Ollama:

```bash
ollama serve
```

In a second terminal, pull the model, install dependencies, and run the gateway:

```bash
ollama pull llama3.2
npm install
cp .env.example .env
npm run dev
```

`PORT` defaults to `3000`, `OLLAMA_BASE_URL` defaults to `http://localhost:11434`, and `OLLAMA_REQUEST_TIMEOUT_MS` defaults to `120000`. Values from a local `.env` file are loaded when present.

Provider connection failures return `503`, provider timeouts return `504`, and other invalid or unsuccessful provider responses return `502`. Public error responses do not include upstream response details.

## Call the API

```bash
curl --request POST http://localhost:3000/v1/chat \
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

The response has a provider-neutral shape:

```json
{
  "message": {
    "role": "assistant",
    "content": "..."
  }
}
```

## Development commands

```bash
npm test
npm run typecheck
npm run build
npm start
```

## Project structure

```text
.
├── src
│   ├── app.ts                 # Fastify app factory and error handling
│   ├── config.ts              # Environment configuration
│   ├── errors.ts              # Application/provider errors
│   ├── server.ts              # Runtime composition and listener
│   ├── providers
│   │   ├── ollama.ts          # Ollama HTTP adapter
│   │   └── provider.ts        # Provider interface
│   ├── routes
│   │   └── chat.ts            # Provider-neutral chat route and schema
│   └── types
│       └── chat.ts            # Provider-neutral request/response types
├── test
│   ├── app.test.ts
│   └── ollama.test.ts
├── Dockerfile
├── docker-compose.yml
├── package.json
├── tsconfig.json
└── .env.example
```

Redis, queues, caching, rate limiting, provider failover, streaming, authentication, metrics, dashboards, and broader observability are intentionally deferred to later milestones.
