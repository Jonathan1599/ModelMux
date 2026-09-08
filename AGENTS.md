You are helping me build **ModelMux**, a production-style LLM inference gateway in Node.js/TypeScript for backend engineering interviews.

The goal is not to maximize features. The goal is to build a clean, understandable backend system that demonstrates:

* API design
* provider abstraction
* rate limiting
* queueing/load leveling
* concurrency control
* caching
* provider failover
* circuit breakers
* streaming
* observability
* load testing
* reasoning about distributed-system tradeoffs

Tech stack:

* Node.js
* TypeScript
* Fastify
* Redis
* Postgres
* pgvector
* BullMQ
* Ollama as the primary provider
* one hosted LLM provider later as fallback
* Prometheus
* Grafana
* k6
* Docker Compose

Current project status:

* `POST /v1/chat` works end-to-end.
* Fastify receives a provider-neutral chat request.
* The route goes through an `LLMProvider` abstraction.
* `OllamaProvider` calls local Ollama.
* Ollama calls have a configurable timeout and return a distinct gateway timeout error.
* The gateway closes gracefully on `SIGINT` and `SIGTERM`.
* A successful response currently looks like:

```json
{
  "message": {
    "role": "assistant",
    "content": "..."
  }
}
```

The first working test request was:

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

Important architecture decisions:

1. Keep provider-specific logic isolated behind the `LLMProvider` abstraction.

2. Do not introduce technologies just for the sake of complexity.

3. BullMQ is intended primarily for asynchronous inference jobs and load leveling. Do not automatically place every synchronous SSE request behind BullMQ unless we deliberately design the return path.

4. Distinguish:

   * per-client rate limiting
   * global/provider concurrency limiting
   * queueing/load leveling

They solve different problems.

5. Caching strategy:

   * exact request caching is safe enough when the complete relevant request matches;
   * semantic caching should NOT be blindly applied to arbitrary chat conversations;
   * semantic caching is intended as a later experiment for repetitive Q&A/RAG workloads;
   * personalized or context-heavy requests should not blindly share semantic-cache responses across users.

6. Semantic caching is an optimization experiment, not a foundational dependency.

7. Do not add:

   * Kubernetes
   * Kafka
   * OAuth
   * frontend
   * billing
   * agents
   * LangChain unless there is a concrete reason
   * unnecessary microservices
   * unnecessary abstraction layers

Planned milestones:

### Milestone 1 — Basic gateway

Already substantially complete:

* Fastify server
* `/v1/chat`
* provider abstraction
* Ollama provider
* request validation
* request IDs
* logging
* error handling
* `/health`
* tests

### Milestone 2 — Authentication and rate limiting

* API keys
* store hashed keys
* API-key metadata / limits
* Redis-backed token bucket or equivalent limiter
* per-key configurable rate limits
* `429`
* `Retry-After`
* limiter tests

### Milestone 3 — Execution protection

* provider/global concurrency limits
* clearly separate concurrency control from request rate limiting
* measure rejection/wait behavior

### Milestone 4 — Async jobs / BullMQ

* BullMQ inference queue
* worker process separate from gateway
* retries with exponential backoff
* job status
* idempotency / in-flight deduplication
* primarily support async job execution first

### Milestone 5 — Provider routing and fault tolerance

* second provider
* provider router
* circuit breaker
* fallback behavior
* provider health state

### Milestone 6 — Streaming

* SSE
* upstream abort on client disconnect
* backpressure handling
* avoid leaking provider/worker capacity
* deliberately decide whether streaming bypasses the queue or uses a worker-to-gateway transport mechanism

### Milestone 7 — Caching

First:

* exact cache

Later experiment:

* semantic cache for Q&A/RAG-style workloads using embeddings + pgvector
* conservative cache eligibility
* similarity threshold testing
* avoid unsafe cross-user personalization reuse

### Milestone 8 — Observability

Metrics should eventually include:

* request count
* request latency
* p50/p95
* rate-limit rejections
* provider latency
* provider errors
* queue depth
* queue wait time
* inference processing time
* retries
* circuit-breaker state
* cache hits/misses
* token usage
* estimated inference cost

Structured logs should preserve a request ID across gateway → queue → worker.

### Milestone 9 — Load testing

Use k6.

Test scenarios should include:

* normal load
* burst load
* overload
* repeated requests
* cacheable Q&A traffic
* provider failure

Capture real numbers rather than inventing resume metrics.

### Milestone 10 — Documentation / polish

* architecture diagram
* setup
* curl examples
* failure behavior
* load-test results
* design decisions
* tradeoffs
* README suitable for an interview reviewer

How I want you to work with me:

* Treat each milestone/day as a guided engineering session.
* Do NOT implement future milestones unless I explicitly ask.
* Before making a major architectural decision, explain:

  1. what problem we're solving,
  2. the main options,
  3. the option you recommend,
  4. why it fits this project.
* Keep explanations concise but technical.
* When modifying code, inspect the existing repository first.
* Preserve working code unless there is a reason to change it.
* Prefer incremental diffs over large rewrites.
* Run tests/typecheck after changes.
* Call out failures or questionable design decisions instead of hiding them.
* If something in the original project plan is a poor design, say so rather than implementing it blindly.
* Optimize for code I can understand and explain in a backend interview.

For now, do not change any code.

First:

1. inspect the existing repository,
2. tell me what Milestone 1 currently contains,
3. identify anything that should be fixed before we move on,
4. then wait for me to choose the next task.
