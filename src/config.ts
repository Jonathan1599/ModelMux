import "dotenv/config";

export interface Config {
  port: number;
  ollamaBaseUrl: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const port = parsePort(env.PORT ?? "3000");
  const ollamaBaseUrl = env.OLLAMA_BASE_URL?.trim() || "http://localhost:11434";

  validateUrl(ollamaBaseUrl, "OLLAMA_BASE_URL");

  return { port, ollamaBaseUrl };
}

function parsePort(value: string): number {
  const port = Number(value);

  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`PORT must be an integer between 1 and 65535; received ${value}`);
  }

  return port;
}

function validateUrl(value: string, name: string): void {
  let url: URL;

  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be a valid URL; received ${value}`);
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error(`${name} must use http or https; received ${value}`);
  }
}
