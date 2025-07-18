import * as mongoDB from "mongodb";
import log from "../utils/log";
import { DATABASE_COLLECTION,DATABASE_NAME, DATABASE_URL } from "../configs/env";
import { Message, sendMessagetoSupervisor } from "../utils/handleMessage";
import { Worker } from "./Worker";
export default class DatabaseInteractionWorker implements Worker {
	private instanceId: string;
	public isBusy: boolean = false;
	private client: mongoDB.MongoClient = new mongoDB.MongoClient(
		DATABASE_URL
	);
	private db: mongoDB.Db = this.client.db(DATABASE_NAME);
	private collection: mongoDB.Collection =
		this.db.collection(DATABASE_COLLECTION);

	constructor() {
		this.instanceId = `DatabaseInteractionWorker-${Date.now()}`;

		this.run().catch((error) => {
			console.error(
				`[DatabaseInteractionWorker] Error in constructor: ${error.message}`
			);
		});
	}
	healthCheck(): void {
		throw new Error("Method not implemented.");
	}
	public getInstanceId(): string {
		return this.instanceId;
	}
	public async run(): Promise<void> {
		try {
			this.client
				.connect()
				.then(() =>
					log(
						"[DatabaseInteractionWorker] Connected to MongoDB",
						"success"
					)
				)
				.catch((error) =>
					log(
						`[DatabaseInteractionWorker] Error connecting to MongoDB: ${error.message}`,
						"error"
					)
				);
			this.listenTask().catch((error) => {
				log(
					`[DatabaseInteractionWorker] Error in run method: ${error.message}`,
					"error"
				);
			});
		} catch (error) {
			console.error(
				`[DatabaseInteractionWorker] Error in run method: ${error.message}`
			);
		}
	}
	async listenTask(): Promise<void> {
		// Simulate listening for tasks
		process.on("message", async (message: Message) => {
			console.log("busy ", this.isBusy);
			if (this.isBusy) {
				log(
					`[DatabaseInteractionWorker] Worker is busy, cannot process new task`,
					"warn"
				);
				sendMessagetoSupervisor({
					...message,
					status: "failed",
					reason: "SERVER_BUSY",
				});
				return;
			}
			this.isBusy = true;
			const { destination, data } = message;
			const dest = destination.filter((d) =>
				d.includes("DatabaseInteractionWorker")
			);
			dest.forEach(async (d) => {
				log(
					`[DatabaseInteractionWorker] Received message for destination: ${d}`,
					"info"
				);
				const destinationSplited = d.split("/");
				const path = destinationSplited[1];
				const subPath = destinationSplited[2];
				const result = await this[path]({ id: subPath, data });
				const { data: res, destination } = result;
				if (result) {
					sendMessagetoSupervisor({
						messageId: message.messageId,
						status: "completed",
						data: res,
						destination: destination,
					});
				}
			});

			this.isBusy = false;
		});
	}

	public async createNewData(data: any): Promise<any> {
		try {
			if (!data || data.length === 0) {
				log(
					"[DatabaseInteractionWorker] No data provided to insert",
					"warn"
				);
				return;
			}
			const insertedData = await this.collection.insertMany(data);
			log(
				`[DatabaseInteractionWorker] Successfully inserted ${insertedData.insertedCount}/${data.length} documents`,
				"success"
			);
			return {
				data: Object.values(insertedData.insertedIds),
				destination: [
					`RabbitMQWorker/produceData/${data[0].projectId}`,
				],
			};
		} catch (error) {
			log(
				`[DatabaseInteractionWorker] Error creating new data: ${error.message}`,
				"error"
			);
		}
	}

	public async getCrawledData(data: any): Promise<any> {
		try {
			const { keyword, start_date ,end_date} = data;
			if (!data || data.length === 0) {
				log(
					"[DatabaseInteractionWorker] No data provided to insert",
					"warn"
				);
				return;
			}
			const query: mongoDB.Filter<mongoDB.Document> = {
				full_text: {
					$regex: keyword,
					$options: "i", // Case-insensitive search
				},
				created_at: {
					$gte: new Date(start_date),
					$lte: new Date(end_date),
				},
			};
			const crawledData = await this.collection.find(query).toArray();
			return {
				data: crawledData,
				destination: [
					`CrawlerWorker/onFechedData`,
				],
			};
		} catch (error) {
			log(
				`[DatabaseInteractionWorker] Error creating new data: ${error.message}`,
				"error"
			);
		}
	}
}

new DatabaseInteractionWorker()