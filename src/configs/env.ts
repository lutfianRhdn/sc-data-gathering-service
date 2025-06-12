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
} = parseEnv(process.env, {
	HEADLESS_MODE: z.boolean().default(true),
	ENABLE_EXPONENTIAL_BACKOFF: z.boolean().default(false),
	DATABASE_URL: z.string().min(1),
	DATABASE_NAME: z.string().min(1),
	DATABASE_COLLECTION: z.string().min(1),
	RABBITMQ_URL: z.string().min(1),
});
