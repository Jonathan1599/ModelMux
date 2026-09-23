# TODO

## Propagate client aborts through concurrency and provider execution

The current request cancellation signal is not passed into the concurrency limiter or `OllamaProvider`.

Current behavior:

- A client that disconnects while waiting remains in the FIFO queue until it receives a permit or reaches `PROVIDER_QUEUE_TIMEOUT_MS`.
- If that request receives a permit, it may still call Ollama after the client has disconnected.
- A client that disconnects while holding a permit continues using provider capacity until Ollama completes, fails, or reaches `OLLAMA_REQUEST_TIMEOUT_MS`.
- The route still releases acquired permits in `finally`, so capacity is eventually recovered.
- Limiter counters classify these requests as admitted or timed out instead of aborted.

Required changes:

- Allow `ConcurrencyLimiter.acquire()` to accept an `AbortSignal`.
- Remove an aborted waiter from the queue and clear its wait timer.
- Avoid invoking the provider when the request is already aborted.
- Extend the provider call contract to accept a cancellation signal without adding Ollama-specific concerns to the route.
- Combine client cancellation with the configured Ollama timeout, for example with `AbortSignal.any()`.
- Preserve permit release in `finally` for every acquired slot.
- Track and log aborted waits and aborted executions separately from queue timeouts and provider failures.

Tests should cover:

- abort while queued;
- abort immediately before admission;
- abort while Ollama holds a slot;
- the next queued request receiving the released slot;
- no duplicate release or active-count underflow;
- aborted requests not being counted as queue timeouts.

This work fits naturally with the streaming/cancellation milestone, but it should be completed before relying on the concurrency limiter under frequent client disconnects.
