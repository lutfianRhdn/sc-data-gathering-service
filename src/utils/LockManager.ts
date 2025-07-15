import { createClient } from "redis";
export class LockManager {
  private redisInstance: ReturnType<typeof createClient>;
  private prefixKey: string = "LOCK_";
  constructor() {
    this.redisInstance = createClient({
		url:
			process.env.REDIS_URL ||
			"redis://socialabs.redis.cache.windows.net:6380",
    password:"bAIWiI43YPRbO0QfOE0gAZoO2XIM2Mz1OAzCaC9hwTI"});
    this.redisInstance.on("error", (err) => {
      console.error("Redis error:", err);
    });
    this.redisInstance.on("connect", () => {
      console.log("Connected to Redis");
    });
  }
  async connect(): Promise<void> {
    if (!this.redisInstance.isOpen) {
      await this.redisInstance.connect();
    }
  }

}
(new LockManager()).connect().then(() => {
  console.log("LockManager connected to Redis");
}).catch((err) => {
  console.error("Error connecting LockManager to Redis:", err);
});
