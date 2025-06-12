import * as mongoDB from "mongodb";
import log from "../utils/log";
import { DATABASE_COLLECTION,DATABASE_NAME, DATABASE_URL } from "../configs/env";
import { Message, sendMessagetoSupervisor } from "../utils/handleMessage";
export default class DatabaseInteractionWorker {
	private instanceId: string;
	public isBusy: boolean = false;
	private client: mongoDB.MongoClient = new mongoDB.MongoClient(DATABASE_URL);
	private db: mongoDB.Db = this.client.db(DATABASE_NAME);
	private collection: mongoDB.Collection =this.db.collection(DATABASE_COLLECTION);

	constructor() {
		this.instanceId = `DatabaseInteractionWorker-${Date.now()}`;

		this.run().catch((error) => {
			console.error(
				`[DatabaseInteractionWorker] Error in constructor: ${error.message}`
			);
		});
	}
	public getInstanceId(): string {
		return this.instanceId;
	}
		public async run(): Promise<void> {
      try {
        this.client
				.connect()
				.then(() => log("[DatabaseInteractionWorker] Connected to MongoDB","success"))
				.catch((error) =>log(`[DatabaseInteractionWorker] Error connecting to MongoDB: ${error.message}`,"error")
        );
        this.listenTask().catch((error) => {
          log(`[DatabaseInteractionWorker] Error in run method: ${error.message}`, "error");
        });
		} catch (error) {
			console.error(
				`[DatabaseInteractionWorker] Error in run method: ${error.message}`
			);
		}
	}
	private async listenTask(): Promise<void> {
		// Simulate listening for tasks
		process.on("message", async (message: Message) => {
			console.log("busy ", this.isBusy)
			if (this.isBusy) {
				log(`[DatabaseInteractionWorker] Worker is busy, cannot process new task`, 'warn');
				sendMessagetoSupervisor({
					...message,
					status: 'failed',
					reason: 'SERVER_BUSY'
				});
				return;
			}
			this.isBusy = true; 
			const { destination, data } = message;
			const destinationSplited = destination.split("/");
			const path = destinationSplited[1]; 
			const subPath = destinationSplited[2];
			switch (path) {
				case "createNewData":
					const insertedData = await this.createNewData(data);
					if (insertedData) {
						sendMessagetoSupervisor({
							...message,
							status: 'completed',
							destination: "RabbitMQWorker",
							data: {projectId:subPath, tweetId:insertedData},
						});
					} 

					break;
				default:
					log(
						`[DatabaseInteractionWorker] Unknown destination: ${destination}`,
						'error'
					);
					return;
			}

			this.isBusy = false;
			
			
		});
	}
	public async createNewData(data: any): Promise<any> {
		try {
			if (!data || data.length === 0) {
				log("[DatabaseInteractionWorker] No data provided to insert", 'warn');
				return;
			}
			const insertedData = await this.collection.insertMany(data);
			log(`[DatabaseInteractionWorker] Successfully inserted ${insertedData.insertedCount}/${data.length} documents`, 'success');
			return Object.values(insertedData.insertedIds);
		} catch (error) {
			log(`[DatabaseInteractionWorker] Error creating new data: ${error.message}`,"error");
		} 
	}
}

new DatabaseInteractionWorker()