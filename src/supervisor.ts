import { count } from "yargs";
import log from "./utils/log";
import {ChildProcess, spawn} from "child_process";
import CrawlerWorker from "./workers/CrawlerWorker";
import { RabbitMQWorker } from "./workers/RabbitMQWorker";
import path from "path";
import { Message } from "./utils/handleMessage";
import { workerConfig } from "./configs/worker";

export default class Supervisor {
	private workers: ChildProcess[] = [];

  

	constructor() {
		// Initialize any necessary resources or configurations
		this.createWorker({
			worker: "RabbitMQWorker",
      count: 1,
      cpu: 1, 
      memory: 1028, 
			config: {
				string_connection:
					"amqp://admin:admin123@70.153.61.68/dev",
			},
    });
    this.createWorker({
      worker: "CrawlerWorker",
      count: 1,
      cpu: 1,
      memory: 1028, 
      config: {}
    });
    
    this.createWorker({
		worker: "DatabaseInteractionWorker",
		count: 1,
		cpu: 1,
		memory: 1028,
		config: {},
    });
		log("[Supervisor] Supervisor initialized");
	}
	createWorker({
		worker,
		count,
		config,
	}: {
      worker: any;
      count: number;
      config: any;
      cpu: number;
      memory?: number;
	}): void {
    if (count <= 0) {
      log("[Supervisor] Worker count must be greater than zero", "error");
      throw new Error("Worker count must be greater than zero");
    }
    log(`[Supervisor] Creating ${count} worker(s) of type ${worker}`, "info");
    for (let i = 0; i < count; i++) {
    
      const workerPath = path.resolve(__dirname, `./workers/${worker}.ts`);
      const runningWorker = spawn("node",[
				path.resolve(
					__dirname,
					"../node_modules/ts-node/dist/bin.js"
				),
				workerPath,
        
			],
        {
          stdio: ["inherit", "inherit", "inherit", "ipc"], env: {
            NODE_OPTIONS: `--max-old-space-size=${config.memory || 1024}`, // Set memory limit
            
      }}
		);
      runningWorker.on('message', (message: any) => this.handleWorkerMessage(message,runningWorker.pid))
      this.workers.push(runningWorker);
    }
    const workerPids = this.workers
      .filter(w => w.spawnargs.find((args) => args.includes(worker)))
      .map((worker: any) => worker.pid);
    log(`[Supervisor]  ${worker} is running on pid: ${ workerPids}`, "success");
  }
  handleWorkerMessage(message:Message,processId:number): void {
    const { messageId, reason, status, destination, data } = message; 
    const workerName = destination.split("/").shift()?.split(".")[0] || "unknown";
    log(`[Supervisor] message received ${messageId} from PID : ${processId}`);
      let availableWorker = this.workers.filter((worker) =>
			worker.spawnargs.find((args) => args.includes(workerName))
		);
        
    if (status === "error") {
		log(`[Supervisor] Error in worker ${processId}: ${reason}`, "error");
		this.restartWorker(
			this.workers.find((worker) => worker.pid === processId)
      );
      return
    }
    
    if (availableWorker.length === 0) {
      log("[Supervisor] No worker found for destination: " + destination, "warn");
      log(`[Supervisor] Creating new worker for destination: ${workerName}`, "info");
      console.log(workerName, workerConfig[workerName])
      this.createWorker({
			worker: workerName,
			count: 1,
			cpu: workerConfig[workerName].cpu || 1,
			memory: workerConfig[workerName].memory || 1028, // in MB
			config: {},
		});
    }

    if (status === "failed" && reason === 'SERVER_BUSY'){
      availableWorker = this.workers.filter((worker) => worker.spawnargs.find(
        (args) => args.includes(workerName) && worker.pid !== processId)
      )};
    
    if (availableWorker.length === 0) {
      log("[Supervisor] No worker found for destination: " + workerName, "warn");
      setTimeout(() => this.handleWorkerMessage({
        ...message,
        status: "completed",
      }, processId), 5000); 
      return
      }
    const worker = availableWorker[0]
    worker.send(message)
    log(`[Supervisor] sended message ${messageId} to worker: ${worker.pid}`, "success");
  }


	handleWorkerError(error: Error): void {
    log(`[Supervisor] Worker error: ${error.message}`, "error");
	}
  restartWorker(worker: any): void {
    const workerName = worker.spawnargs[worker.spawnargs.length - 1].split(/[/\\]/).pop().split(".")[0];
    log(`[Supervisor] Restarting worker : ${workerName} ( PID: ${worker.pid})`, "warn");
    worker.kill();
    this.createWorker({
      worker: workerName,
      count: 1,
      cpu: workerConfig[workerName].cpu || 1,
      memory: workerConfig[workerName].memory || 1028, // in MB
      config: {},
    });
	}
}