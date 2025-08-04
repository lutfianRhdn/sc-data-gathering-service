export interface Worker {
	run(): Promise<void>;
	listenTask(): Promise<void>;
}