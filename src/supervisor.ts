import log from "./utils/log";
import { ChildProcess, spawn,execSync } from "child_process";
import path from "path";
import { Message } from "./utils/handleMessage";
import { workerConfig } from "./configs/worker";
import { Timestamp } from "mongodb";
import { RABBITMQ_URL } from "./configs/env";
import {tasklist} from "tasklist";
import psList from "ps-list";
interface WorkerHealthInterFace {
	isHealthy: boolean;
	workerNameId: string;
	timestamp: Timestamp;
}

interface CreateWorkerOptions {
	worker: string;
	count: number;
	config: any;
}

type PendingMessage = Message & { timestamp: number };

export default class Supervisor {
	private workers: ChildProcess[] = [];
	private workersHealth: Record<number, WorkerHealthInterFace> = {};
	private pendingMessages: Record<string, PendingMessage[]> = {};

	constructor() {
		this.createWorker({
			worker: "CrawlerWorker",
			count: workerConfig['CrawlerWorker'].count,
			config: workerConfig['CrawlerWorker'].config,
		});
		this.createWorker({
			worker: "DatabaseInteractionWorker",
			count: workerConfig['DatabaseInteractionWorker'].count,
			config: workerConfig['DatabaseInteractionWorker'].config,
		});

		this.createWorker({
			worker: "RabbitMQWorker",
			count: workerConfig['RabbitMQWorker'].count,
			config: workerConfig['RabbitMQWorker'].config,
		});
		// setInterval(() => this.checkWorkerHealth(), 10000); // Check worker health every 10 seconds
		log("[Supervisor] Supervisor initialized");
	}

	createWorker({
		worker,
		count,
		config,
	}: CreateWorkerOptions): void {
		if (count <= 0) {
			log(
				"[Supervisor] Worker count must be greater than zero",
				"error"
			);
			throw new Error("Worker count must be greater than zero");
		}
		log(
			`[Supervisor] Creating ${count} worker(s) of type ${worker}`,
			"info"
		);

		for (let i = 0; i < count; i++) {
			const workerPath = path.resolve(
				__dirname,
				`./workers/${worker}.ts`
			);

			const runningWorker = spawn(
				process.execPath,
				[
					path.resolve(
						__dirname,
						"../node_modules/ts-node/dist/bin.js"
					),
					workerPath,
				],
				{
					stdio: ["inherit", "inherit", "inherit", "ipc"],
					env: { ...config },
				}
			);

			// === Tambahkan event untuk cleanup workers array ===
			runningWorker.on("exit", () => {
				this.workers = this.workers.filter(
					(w) => w.pid !== runningWorker.pid
				);
				log(
					`[Supervisor] Worker exited. PID: ${runningWorker.pid}`,
					"warn"
				);
				this.createWorker({worker:worker, count:count, config});
			});

			runningWorker.on("message", (message: any) =>
				this.handleWorkerMessage(message, runningWorker.pid!)
			);
			this.workers.push(runningWorker);
		}

		const workerPids = this.workers
			.filter((w) => w.spawnargs.some((args) => args.includes(worker)))
			.map((w) => w.pid);

		log(
			`[Supervisor] ${worker} is running on pid: ${workerPids}`,
			"success"
		);

		// Saat selesai createWorker, cek jika ada pendingMessages untuk worker ini
		this.resendPendingMessages(worker);
	}


	handleWorkerMessage(message: Message, processId: number): void {
		const { messageId, reason, status, destination } = message;

		destination.forEach((dest) => {
			if (dest !== "supervisor") {
				message.destination = destination.filter((d) => d === dest);
				this.handleSendMessageWorker(processId, message);
				return;
			}
			// Bisa tambahkan logic khusus supervisor di sini kalau ada
			if (status === "healthy") {
				this.workersHealth[processId] = {
					isHealthy: true,
					workerNameId: message.data.instanceId,
					timestamp: new Timestamp({
						t: Math.floor(Date.now() / 1000),
						i: 0,
					}),
				};
			}

			// Jika pesan status "completed", hapus dari pending
			if (status === "completed" && destination) {
				const workerName =
					dest.split("/")?.[0]?.split(".")?.[0] ?? "";
				this.removePendingMessage(workerName, messageId);
			}
		});
	}

	handleSendMessageWorker(processId: number, message: Message): void {
		const { messageId, reason, status, destination } = message;
		destination.forEach((dest) => {
			const workerName =
				dest?.split("/")?.[0]?.split(".")?.[0] ?? "unknown";
			console.log(
				`[Supervisor] Handling message for worker: ${workerName}`
			);

			log(
				`[Supervisor] message received ${messageId} from PID : ${processId}`
			);

			let availableWorkers = this.workers.filter((worker) => {
				const usSameWorkerName = worker.spawnargs.some((args) =>
					args.includes(workerName)
				);
				const isAlive = this.isWorkerAlive(worker);
				const isReady =
					execSync(`ps -o state= -p ${worker.pid}`)
						.toString()
						.trim() === "R";
				return usSameWorkerName && isAlive && !isReady;
			});
			// Track message sebelum dikirim
			this.trackPendingMessage(workerName, message);

			if (status === "error") {
				log(
					`[Supervisor] Error in worker ${processId}: ${reason}`,
					"error"
				);
				const worker = this.workers.find(
					(w) => w.pid === processId
				);
				if (worker) this.restartWorker(worker);
				return;
			}

			if (availableWorkers.length === 0) {
				log(
					"[Supervisor] No worker found for destination: " +
						destination,
					"warn"
				);
				log(
					`[Supervisor] Creating new worker for destination: ${workerName}`,
					"info"
				);
				this.createWorker({
					worker: workerName,
					...workerConfig[workerName],
				});
				return;
			}

			if (status === "failed" && reason === "SERVER_BUSY") {
				availableWorkers = availableWorkers.filter(
					(worker) => worker.pid !== processId
				);
			}

			if (availableWorkers.length === 0) {
				log(
					"[Supervisor] No available worker for destination: " +
						workerName,
					"warn"
				);
				setTimeout(() => {
					this.handleWorkerMessage(
						{ ...message, status: "completed" },
						processId
					);
				}, 5000);
				return;
			}

			const targetWorker = availableWorkers[0];
			if (this.isWorkerAlive(targetWorker)) {
				log(
					`[Supervisor] Sending message ${messageId} to worker: ${workerName} (${targetWorker.pid})`
				);
				targetWorker.send(message);
				log(
					`[Supervisor] sent message ${messageId} to worker: ${workerName} (${targetWorker.pid})`,
					"success"
				);
			} else {
				log(
					`[Supervisor] Tried to send message to dead worker!`,
					"error"
				);
			}
		});
	}

	handleWorkerError(error: Error): void {
		log(`[Supervisor] Worker error: ${error.message}`, "error");
	}

	restartWorker(worker: ChildProcess): void {
		const workerName = worker.spawnargs[worker.spawnargs.length - 1]
			.split(/[/\\]/)
			.pop()!
			.split(".")[0];

		log(
			`[Supervisor] Restarting worker: ${workerName} (PID: ${worker.pid})`,
			"warn"
		);
		worker.kill();
		this.createWorker({
			worker: workerName,
			...workerConfig[workerName],
		});

		// Setelah worker baru dibuat, resend semua message pending untuk worker ini
		this.resendPendingMessages(workerName);
	}

	// =========================
	// TRACKING LOGIC
	// =========================

	private trackPendingMessage(workerName: string, message: Message) {
		if (!workerName) return;
		if (!this.pendingMessages[workerName]) {
			this.pendingMessages[workerName] = [];
		}
		// Hindari duplikasi messageId
		if (
			!this.pendingMessages[workerName].some(
				(m) => m.messageId === message.messageId
			)
		) {
			this.pendingMessages[workerName].push({
				...message,
				timestamp: Date.now(),
			});
		}
	}

	private removePendingMessage(workerName: string, messageId: string) {
		if (!workerName || !this.pendingMessages[workerName]) return;
		this.pendingMessages[workerName] = this.pendingMessages[
			workerName
		].filter((msg) => msg.messageId !== messageId);
	}

	private resendPendingMessages(workerName: string) {
		const messages = this.pendingMessages[workerName];
		if (!messages || messages.length === 0) return;
		log(
			`[Supervisor] Resending ${messages.length} pending messages to new worker: ${workerName}`,
			"info"
		);

		// Cari worker yang alive
		const availableWorker = this.workers.find(
			(worker) =>
				worker.spawnargs.some((args) =>
					args.includes(workerName)
				) && this.isWorkerAlive(worker)
		);

		if (!availableWorker) {
			log(
				`[Supervisor] No available (alive) worker to resend messages for: ${workerName}`,
				"warn"
			);
			return;
		}

		messages.forEach((msg) => {
			try {
				availableWorker.send(msg);
				log(
					`[Supervisor] resent message ${msg.messageId} to worker: ${availableWorker.pid}`,
					"success"
				);
			} catch (e: any) {
				log(
					`[Supervisor] Failed to resend message ${msg.messageId} to worker: ${availableWorker.pid} (${e.message})`,
					"error"
				);
			}
		});
	}

	private isWorkerAlive(worker: ChildProcess): boolean {
		return worker && worker.exitCode === null && !worker.killed;
	}
}
