import { buildApp } from "./app.js";
import { loadConfig } from "./config.js";
import { OllamaProvider } from "./providers/ollama.js";

const config = loadConfig();
const provider = new OllamaProvider(
  config.ollamaBaseUrl,
  config.ollamaRequestTimeoutMs,
);
const app = buildApp({ provider });
let isShuttingDown = false;

async function start(): Promise<void> {
  try {
    await app.listen({ port: config.port, host: "0.0.0.0" });
  } catch (error) {
    app.log.fatal({ err: error }, "Failed to start gateway");
    process.exitCode = 1;
  }
}

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  app.log.info({ signal }, "Shutting down gateway");

  try {
    await app.close();
  } catch (error) {
    app.log.error({ err: error }, "Failed to shut down gateway cleanly");
    process.exitCode = 1;
  }
}

process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));

void start();
