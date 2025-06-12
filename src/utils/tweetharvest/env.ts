import { config } from "dotenv";
import { parseEnv, z } from "znv";

config();

export const { HEADLESS_MODE, ENABLE_EXPONENTIAL_BACKOFF, DATABASE_URL } =
	parseEnv(process.env, {
		HEADLESS_MODE: z.boolean().default(true),
		ENABLE_EXPONENTIAL_BACKOFF: z.boolean().default(false),
		DATABASE_URL: z.string().min(1).optional(),
	});
