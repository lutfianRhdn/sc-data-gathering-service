import { DATABASE_COLLECTION, DATABASE_NAME, DATABASE_URL, RABBITMQ_URL, REDIS_URL, REDIS_PASSWORD, REDIS_PORT, REDIS_USERNAME } from "./env";
export const workerConfig = {
	CrawlerWorker: {
		count: 1,
		config: {
			redisHost: REDIS_URL,
			redisPort: REDIS_PORT,
			redisUsername: REDIS_USERNAME,
			redisPassword: REDIS_PASSWORD || "default",
		},
	},
	RabbitMQWorker: {
		count: 1,
		config: {
			consumeQueue: "projectQueue",
			consumeCompensationQueue: "projectCompensationQueueue",
			produceQueue: "dataGatheringQueue",
			produceCompensationQueue: "dataGatheringCompensationQueueue",
			rabbitMqUrl: RABBITMQ_URL,
		},
	},
	DatabaseInteractionWorker: {
		count: 1,
		config: {
			DATABASE_URL,
			DATABASE_NAME,
			DATABASE_COLLECTION,
		},
	},
};
