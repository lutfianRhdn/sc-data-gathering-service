import { createClient } from "redis";
export default class LockManager {
  private redisInstance: ReturnType<typeof createClient>;
  private prefixKey: string = "LOCK_";
  constructor() {
    this.redisInstance = createClient({
		url: process.env.REDIS_URL ||"redis://localhost:6979"});
    this.redisInstance.on("error", (err) => {
      console.error("Redis error:", err);
    });
    if(!this.redisInstance.isOpen) {
      this.redisInstance.connect().catch(err => {
        console.error("Failed to connect to Redis:", err);
      });
    }
  }
  public aquireLock(key: string): void {
    const now = Date.now();
    this.redisInstance.set(this.prefixKey+key, JSON.stringify({ timestamp: now }), {
      NX: true,
      EX: 60*100 // Lock expires after 60 seconds
    }).then((result) => {
      if (result === null) {
        console.log(`Lock for ${key} already exists.`);
      } else {
        console.log(`Lock for ${key} acquired.`);
      }
    }).catch((err) => {
      console.error("Error acquiring lock:", err);
    });

  }
  public releaseLock(key: string): void {
    this.redisInstance.del(this.prefixKey+key).then((result) => {
      if (result === 1) {
        console.log(`Lock for ${key} released.`);
      } else {
        console.log(`No lock found for ${key} to release.`);
      }
    }).catch((err) => {
      console.error("Error releasing lock:", err);
    });
  }
  public async checkLock(key: string): Promise<boolean> {
    return await  this.redisInstance.get(key).then((result) => {
      if (result) {
        console.log(`Lock for ${key} exists.`);
        return true;
      } else {
        console.log(`No lock found for ${key}.`);
        return false;
      }
    }).catch((err) => {
      console.error("Error checking lock:", err);
      return false;
    });
  }

  public relaseAllLocks(): void {
    this.redisInstance.keys(`${this.prefixKey}*`).then((keys) => {
      if (keys.length === 0) {
        console.log("No locks to release.");
        return;
      }
      const multi = this.redisInstance.multi();
      keys.forEach((key) => {
        multi.del(key);
      });
      multi.exec().then(() => {
        console.log("All locks released.");
      }).catch((err) => {
        console.error("Error releasing all locks:", err);
      });
    }).catch((err) => {
      console.error("Error fetching locks:", err);
    });
  }
  public async getAllLocks(key:String): Promise<string[]> {
    try {
      const keys = await this.redisInstance.keys(`${this.prefixKey}${key}:*`);
      if (keys.length === 0) {
        console.log("No locks found.");
        return [];
      }
      return keys as string[];
    } catch (err) {
      console.error("Error fetching locks:", err);
      return [];
    }
  }

}
