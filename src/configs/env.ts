import { config } from "dotenv";
import { parseEnv, z } from "znv";

config();

export const {
	HEADLESS_MODE,
	ENABLE_EXPONENTIAL_BACKOFF,
	DATABASE_URL,
  DATABASE_COLLECTION,
  RABBITMQ_URL,
	DATABASE_NAME,
} = process.env;
