import type { FastifyReply, FastifyRequest } from "fastify";
import { RequestAbortedError } from "../cancellation";

export function requestCancellation(request: FastifyRequest, response: FastifyReply, shutdown: AbortSignal) {
  const controller = new AbortController();
  const abort = () => controller.abort(new RequestAbortedError());
  const onClose = () => { if (!response.raw.writableFinished) abort(); };
  // IncomingMessage.close also fires when a normal request body finishes.
  // ServerResponse.close detects disconnection while waiting for the answer.
  request.raw.once("aborted", abort);
  response.raw.once("close", onClose);
  response.raw.once("error", abort);
  shutdown.addEventListener("abort", abort, { once: true });
  if (request.raw.aborted || response.raw.destroyed || shutdown.aborted) abort();
  return {
    signal: controller.signal,
    abort,
    dispose() {
      request.raw.off("aborted", abort);
      response.raw.off("close", onClose);
      response.raw.off("error", abort);
      shutdown.removeEventListener("abort", abort);
    },
  };
}
