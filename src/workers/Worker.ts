export interface Worker {
  run(): Promise<void>;
  linstenTask(task: any): Promise<void>;
}