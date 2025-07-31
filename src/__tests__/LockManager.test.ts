import LockManager from '../utils/LockManager';
import { createClient } from 'redis';

// Mock Redis client
jest.mock('redis', () => ({
  createClient: jest.fn()
}));

describe('LockManager', () => {
  let lockManager: LockManager;
  let mockRedisClient: any;

  beforeEach(() => {
    // Create mock Redis client with all required methods
    mockRedisClient = {
      isOpen: false,
      connect: jest.fn().mockResolvedValue(undefined),
      set: jest.fn(),
      del: jest.fn(),
      get: jest.fn(),
      keys: jest.fn(),
      multi: jest.fn().mockReturnValue({
        del: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([])
      }),
      on: jest.fn()
    };

    (createClient as jest.MockedFunction<typeof createClient>).mockReturnValue(mockRedisClient);
    
    lockManager = new LockManager();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('Constructor', () => {
    it('should create Redis client with correct configuration', () => {
      expect(createClient).toHaveBeenCalledWith({
        username: "default",
        password: "ZwvQ63UoMnX3TvNGewgyCGCpejC0dqzA",
        socket: {
          host: "redis-13541.c54.ap-northeast-1-2.ec2.redns.redis-cloud.com",
          port: 13541,
        },
      });
    });

    it('should setup error handler', () => {
      expect(mockRedisClient.on).toHaveBeenCalledWith('error', expect.any(Function));
    });

    it('should connect to Redis if not already open', () => {
      expect(mockRedisClient.connect).toHaveBeenCalled();
    });

    it('should handle connection errors gracefully', () => {
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      mockRedisClient.connect.mockRejectedValue(new Error('Connection failed'));
      
      new LockManager();
      
      // Clean up
      errorSpy.mockRestore();
    });
  });

  describe('aquireLock', () => {
    it('should acquire lock successfully', async () => {
      const key = 'test-key';
      mockRedisClient.set.mockResolvedValue('OK');

      await lockManager.aquireLock(key);

      expect(mockRedisClient.set).toHaveBeenCalledWith(
        'LOCK_test-key',
        expect.stringContaining('{"timestamp":'),
        {
          NX: true,
          EX: 6000 // 60*100 seconds
        }
      );
    });

    it('should handle existing lock', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.set.mockResolvedValue(null);

      await lockManager.aquireLock(key);

      expect(consoleSpy).toHaveBeenCalledWith('Lock for test-key already exists.');
      consoleSpy.mockRestore();
    });

    it('should handle Redis errors when acquiring lock', async () => {
      const key = 'test-key';
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      mockRedisClient.set.mockRejectedValue(new Error('Redis error'));

      await lockManager.aquireLock(key);

      expect(errorSpy).toHaveBeenCalledWith('Error acquiring lock:', expect.any(Error));
      errorSpy.mockRestore();
    });
  });

  describe('releaseLock', () => {
    it('should release lock successfully', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.del.mockResolvedValue(1);

      await lockManager.releaseLock(key);

      expect(mockRedisClient.del).toHaveBeenCalledWith('LOCK_test-key');
      expect(consoleSpy).toHaveBeenCalledWith('Lock for test-key released.');
      consoleSpy.mockRestore();
    });

    it('should handle non-existent lock when releasing', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.del.mockResolvedValue(0);

      await lockManager.releaseLock(key);

      expect(consoleSpy).toHaveBeenCalledWith('No lock found for test-key to release.');
      consoleSpy.mockRestore();
    });

    it('should handle Redis errors when releasing lock', async () => {
      const key = 'test-key';
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      mockRedisClient.del.mockRejectedValue(new Error('Redis error'));

      await lockManager.releaseLock(key);

      expect(errorSpy).toHaveBeenCalledWith('Error releasing lock:', expect.any(Error));
      errorSpy.mockRestore();
    });
  });

  describe('checkLock', () => {
    it('should return true when lock exists', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.get.mockResolvedValue('{"timestamp": 1234567890}');

      const result = await lockManager.checkLock(key);

      expect(result).toBe(true);
      expect(consoleSpy).toHaveBeenCalledWith('Lock for test-key exists.');
      consoleSpy.mockRestore();
    });

    it('should return false when lock does not exist', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.get.mockResolvedValue(null);

      const result = await lockManager.checkLock(key);

      expect(result).toBe(false);
      expect(consoleSpy).toHaveBeenCalledWith('No lock found for test-key.');
      consoleSpy.mockRestore();
    });

    it('should handle Redis errors when checking lock', async () => {
      const key = 'test-key';
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      mockRedisClient.get.mockRejectedValue(new Error('Redis error'));

      const result = await lockManager.checkLock(key);

      expect(result).toBe(false);
      expect(errorSpy).toHaveBeenCalledWith('Error checking lock:', expect.any(Error));
      errorSpy.mockRestore();
    });
  });

  describe('relaseAllLocks', () => {
    it('should release all locks successfully', async () => {
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      const keys = ['LOCK_key1', 'LOCK_key2', 'LOCK_key3'];
      mockRedisClient.keys.mockResolvedValue(keys);
      
      const mockMulti = {
        del: jest.fn().mockReturnThis(),
        exec: jest.fn().mockResolvedValue([])
      };
      mockRedisClient.multi.mockReturnValue(mockMulti);

      await lockManager.relaseAllLocks();

      expect(mockRedisClient.keys).toHaveBeenCalledWith('LOCK_*');
      expect(mockMulti.del).toHaveBeenCalledTimes(3);
      expect(mockMulti.exec).toHaveBeenCalled();
      expect(consoleSpy).toHaveBeenCalledWith('All locks released.');
      consoleSpy.mockRestore();
    });

    it('should handle no locks to release', async () => {
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.keys.mockResolvedValue([]);

      await lockManager.relaseAllLocks();

      expect(consoleSpy).toHaveBeenCalledWith('No locks to release.');
      consoleSpy.mockRestore();
    });

    it('should handle Redis errors when fetching keys', async () => {
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      mockRedisClient.keys.mockRejectedValue(new Error('Redis error'));

      await lockManager.relaseAllLocks();

      expect(errorSpy).toHaveBeenCalledWith('Error fetching locks:', expect.any(Error));
      errorSpy.mockRestore();
    });

    it('should handle Redis errors when releasing all locks', async () => {
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      const keys = ['LOCK_key1', 'LOCK_key2'];
      mockRedisClient.keys.mockResolvedValue(keys);
      
      const mockMulti = {
        del: jest.fn().mockReturnThis(),
        exec: jest.fn().mockRejectedValue(new Error('Exec error'))
      };
      mockRedisClient.multi.mockReturnValue(mockMulti);

      await lockManager.relaseAllLocks();

      expect(errorSpy).toHaveBeenCalledWith('Error releasing all locks:', expect.any(Error));
      errorSpy.mockRestore();
    });
  });

  describe('getAllLocks', () => {
    it('should return all locks for a key prefix', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      const expectedKeys = ['LOCK_test-key:1', 'LOCK_test-key:2'];
      mockRedisClient.keys.mockResolvedValue(expectedKeys);

      const result = await lockManager.getAllLocks(key);

      expect(mockRedisClient.keys).toHaveBeenCalledWith('LOCK_test-key:*');
      expect(result).toEqual(expectedKeys);
      expect(consoleSpy).toHaveBeenCalledWith('Fetching all locks for key: LOCK_test-key');
      consoleSpy.mockRestore();
    });

    it('should return empty array when no locks found', async () => {
      const key = 'test-key';
      const consoleSpy = jest.spyOn(console, 'log').mockImplementation();
      mockRedisClient.keys.mockResolvedValue([]);

      const result = await lockManager.getAllLocks(key);

      expect(result).toEqual([]);
      expect(consoleSpy).toHaveBeenCalledWith('No locks found.');
      consoleSpy.mockRestore();
    });

    it('should handle Redis errors when getting all locks', async () => {
      const key = 'test-key';
      const errorSpy = jest.spyOn(console, 'error').mockImplementation();
      mockRedisClient.keys.mockRejectedValue(new Error('Redis error'));

      const result = await lockManager.getAllLocks(key);

      expect(result).toEqual([]);
      expect(errorSpy).toHaveBeenCalledWith('Error fetching locks:', expect.any(Error));
      errorSpy.mockRestore();
    });
  });
});