export interface Worker {
	run(): Promise<void>;
	healthCheck(): void;
	listenTask(): Promise<void>;
}