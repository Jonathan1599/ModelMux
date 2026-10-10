import type { Config } from "../config";
import { CircuitBreaker } from "./circuit-breaker";
import { OllamaProvider } from "./ollama";
import { OpenAIProvider } from "./openai";
import { ProviderRouter } from "./router";
import type { MetricSink } from "../observability/telemetry";

// Gateway and worker use the same policy, each with its own in-memory circuits.
export function createProvider(config: Config, metrics?: MetricSink): ProviderRouter {
  const primary = {
    name: "ollama",
    provider: new OllamaProvider(config.ollamaBaseUrl, config.ollamaRequestTimeoutMs),
    circuit: new CircuitBreaker(config.circuitBreaker),
  };
  const fallback = config.openai ? {
    name: "openai",
    provider: new OpenAIProvider(config.openai.apiKey, config.openai.requestTimeoutMs),
    circuit: new CircuitBreaker(config.circuitBreaker),
    modelMap: config.openai.modelMap,
  } : undefined;

  return new ProviderRouter(primary, fallback, metrics);
}
