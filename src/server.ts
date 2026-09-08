import { buildApp } from "./app.js";
import { loadConfig } from "./config.js";
import { OllamaProvider } from "./providers/ollama.js";

const config = loadConfig();
const provider = new OllamaProvider(config.ollamaBaseUrl);
const app = buildApp({ provider });

async function start(): Promise<void> {
  try {
    await app.listen({ port: config.port, host: "0.0.0.0" });
  } catch (error) {
    app.log.fatal({ err: error }, "Failed to start gateway");
    process.exitCode = 1;
  }
}

void start();
