import * as amqp from "amqplib";
import { Worker } from "./Worker";
import Supervisor from "../supervisor";
import { v4 as uuidv4 } from "uuid";
import log from "../utils/log";
import { Message, sendMessage, sendMessagetoSupervisor } from "../utils/handleMessage";
import { RABBITMQ_URL } from "../configs/env";
export class RabbitMQWorker implements Worker {
	private instanceId: string;
	public isBusy: boolean = false; // Add isBusy property to track worker status
	private string_connection: string;
	private connection: amqp.ChannelModel | null = null; // Use null to indicate uninitialized state

	private consumeQueue: string = process.env.consumeQueue;
	private consumeCompensationQueue: string =process.env.consumeCompensationQueue; // Add compensation queue
	private consumeChannel: any;

	private produceQueue: string = 'dataGatheringQueue';
	private produceCompensationQueue: string =
		process.env.dataGatheringCompensationQueue; // Add compensation queue
	private produceChannel: amqp.Channel;

	constructor() {
		this.instanceId = `RabbitMqWorker-${uuidv4()}`;
		this.string_connection =
			RABBITMQ_URL ||
			"amqp://admin:admin123@70.153.61.68:5672/dev";
		this.run().catch((error) => {
			log(
				`[RabbitMQWorker] Error in run method: ${error.message}`,
				"error"
			);
		});
	}
	healthCheck(): void {
		setInterval(
			() =>
				sendMessagetoSupervisor({
					messageId: uuidv4(),
					status: "healthy",
					data: {
						instanceId: this.instanceId,
						timestamp: new Date().toISOString(),
					},
				}),
			10000
		);
	}

	public getInstanceId(): string {
		return this.instanceId;
	}
	public async run(): Promise<void> {
		try {
			if (!this.string_connection) {
				throw new Error("Connection string is not provided");
			}
			this.connection = await amqp.connect(this.string_connection, {
				heartbeat: 60,
				timeout: 10000,
			});
			log(
				`[RabbitMQWorker] Connected to RabbitMQ at ${this.string_connection}`,
				"success"
			);
			this.connection.on("error", (error: Error) => {
				log(
					`[RabbitMQWorker] Connection error: ${error.message}`,
					"error"
				);
			});
			this.connection.on("close", (reason) => {
				sendMessagetoSupervisor({
					messageId: uuidv4(),
					status: "error",
					reason: reason.message || reason.toString(),
					data: [],
				});
				log(
					`[RabbitMQWorker] Connection closed ${reason}`,
					"error"
				);
			});
			this.connection.on("blocked", (reason: string) => {
				sendMessagetoSupervisor({
					messageId: uuidv4(),
					status: "error",
					data: [],
					reason: reason,});
				log(
					`[RabbitMQWorker] Connection blocked: ${reason}`,
					"error"
				);
			});
			this.healthCheck();
			this.listenTask().catch((error) =>
				log(
					`[RabbitMQWorker] Error in linstenTask method: ${error.message}`,
					"error"
				)
			);
			log(
				`[RabbitMQWorker] instanceId: ${this.instanceId} is running`,
				"success"
			);
			await this.consumeMessage(this.consumeQueue);
		} catch (error) {
			log(
				`[RabbitMQWorker] Failed to run worker: ${error.message}`,
				"error"
			);
			throw error;
		}
	}
	public async consumeMessage(queueName: string): Promise<void> {
		if (!this.connection) {
			log("[RabbitMQWorker] Connection is not established", "error");
			throw new Error("Connection is not established");
		}
		this.consumeChannel = await this.connection.createChannel();
		await this.consumeChannel.assertQueue(queueName, {
			durable: true,
		});
		log(
			`[RabbitMQWorker] Listening to consume queue: ${queueName}`,
			"info"
		);
		this.consumeChannel.consume(
			queueName,
			(msg) => {
				if (msg !== null) {
					const messageContent = msg.content.toString();
					if (queueName === this.consumeQueue) {
						sendMessagetoSupervisor({
							messageId: uuidv4(),
							status: "completed",
							data: JSON.parse(messageContent),
							destination: ["CrawlerWorker/crawling"],
						});

					} else if (
						queueName === this.consumeCompensationQueue
					) {
						// sendMessagetoSupervisor({
						// 	messageId: uuidv4(),
						// 	status: "error",
						// 	reason: "ROLLBACK",
						// 	data: JSON.parse(messageContent),
						// 	destination: "DatabaseInteractionWorker",
						// });
					}
				}
			},
			{ noAck: true }
		);
	}
	public async produceMessage(
		message: any,
		queueName: string = this.produceQueue
	): Promise<void> {
		try {
			this.produceChannel = await this.connection.createChannel();
			await this.produceChannel.assertQueue(queueName, {
				durable: true,
			});
			if (!this.produceChannel) {
				throw new Error("Produce channel is not initialized");
			}
			const messageBuffer = Buffer.from(JSON.stringify(message));
			this.produceChannel.sendToQueue(
				queueName, // Use the specified queue name
				messageBuffer,
				{ persistent: true }
			);
		} catch (error) {
			console.error("Failed to send message to RabbitMQ:", error);
		}
	}
	async listenTask(): Promise<void> {
		try {
			process.on("message", async (message: Message) => {
				const { messageId, data, status, reason } = message;
				log(
					`[RabbitMQWorker] Received message: ${messageId}`,
					"info"
				);
				if (status === "failed" && reason === "NO_TWEET_FOUND") {
					this.produceMessage(
						data,
						this.consumeCompensationQueue
					)
						.then(() => {
							log(
								`[RabbitMQWorker] Message ${messageId} sent to compensation queue`,
								"info"
							);
						})
						.catch((error) => {
							log(
								`[RabbitMQWorker] Error sending message ${messageId} to compensation queue: ${error.message}`,
								"error"
							);
						});
					return;
				}
				this.produceMessage(data, this.produceQueue)
					.then(() => {
						log(
							`[RabbitMQWorker] Message ${messageId} sent to consume queue`,
							"info"
						);
					})
					.catch((error) => {
						log(
							`[RabbitMQWorker] Error sending message ${messageId} to consume queue: ${error.message}`,
							"error"
						);
					});
			});
			// await this.produceMessage(task);
		} catch (error) {
			log(
				`[RabbitMQWorker] Error listening to task: ${error.message}`,
				"error"
			);
			throw error;
		}
	}
}

new RabbitMQWorker()