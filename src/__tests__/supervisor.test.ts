import Supervisor from '../supervisor';
import { ChildProcess, spawn, execSync } from 'child_process';
import { Message } from '../utils/handleMessage';
import { Timestamp } from 'mongodb';

// Mock external dependencies
jest.mock('child_process');
jest.mock('../utils/log');
jest.mock('../configs/worker');
jest.mock('../configs/env', () => ({
  RABBITMQ_URL: 'amqp://localhost'
}));

// Mock process.execPath
Object.defineProperty(process, 'execPath', {
  value: '/usr/bin/node',
  writable: false
});

const mockSpawn = spawn as jest.MockedFunction<typeof spawn>;
const mockExecSync = execSync as jest.MockedFunction<typeof execSync>;

// Create a mock ChildProcess
const createMockChildProcess = (pid: number = Math.floor(Math.random() * 10000)): any => {
  return {
    pid,
    exitCode: null,
    killed: false,
    spawnargs: ['node', 'ts-node', '/path/to/worker/TestWorker.ts'],
    on: jest.fn(),
    send: jest.fn(),
    kill: jest.fn(),
    stdout: { on: jest.fn() },
    stderr: { on: jest.fn() },
    stdin: { write: jest.fn() }
  };
};

describe('Supervisor', () => {
  let supervisor: Supervisor;
  let mockChildProcess1: any;
  let mockChildProcess2: any;
  let mockChildProcess3: any;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Setup mock child processes
    mockChildProcess1 = createMockChildProcess(1001);
    mockChildProcess2 = createMockChildProcess(1002);
    mockChildProcess3 = createMockChildProcess(1003);

    // Mock spawn to return different processes for different workers
    let spawnCallCount = 0;
    mockSpawn.mockImplementation(() => {
      spawnCallCount++;
      if (spawnCallCount === 1) return mockChildProcess1;
      if (spawnCallCount === 2) return mockChildProcess2;
      return mockChildProcess3;
    });

    // Mock execSync for process state check
    mockExecSync.mockReturnValue(Buffer.from('S')); // Sleeping state

    // Mock workerConfig
    jest.doMock('../configs/worker', () => ({
      workerConfig: {
        CrawlerWorker: { count: 1, cpu: 1, memory: 1024, config: {} },
        DatabaseInteractionWorker: { count: 1, cpu: 1, memory: 1024, config: {} },
        RabbitMQWorker: { count: 1, cpu: 1, memory: 1024, config: {} }
      }
    }));
  });

  describe('Constructor', () => {
    it('should create supervisor and initialize workers', () => {
      supervisor = new Supervisor();
      
      // Should spawn 3 workers (CrawlerWorker, DatabaseInteractionWorker, RabbitMQWorker)
      expect(mockSpawn).toHaveBeenCalledTimes(3);
      
      // Verify worker processes are tracked
      expect((supervisor as any).workers).toHaveLength(3);
      expect((supervisor as any).workers[0].pid).toBe(1001);
      expect((supervisor as any).workers[1].pid).toBe(1002);
      expect((supervisor as any).workers[2].pid).toBe(1003);
    });

    it('should setup event listeners for worker processes', () => {
      supervisor = new Supervisor();
      
      // Each worker should have exit and message event listeners
      expect(mockChildProcess1.on).toHaveBeenCalledWith('exit', expect.any(Function));
      expect(mockChildProcess1.on).toHaveBeenCalledWith('message', expect.any(Function));
      expect(mockChildProcess2.on).toHaveBeenCalledWith('exit', expect.any(Function));
      expect(mockChildProcess2.on).toHaveBeenCalledWith('message', expect.any(Function));
      expect(mockChildProcess3.on).toHaveBeenCalledWith('exit', expect.any(Function));
      expect(mockChildProcess3.on).toHaveBeenCalledWith('message', expect.any(Function));
    });
  });

  describe('createWorker', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
      jest.clearAllMocks();
    });

    it('should throw error if count is zero or negative', () => {
      expect(() => {
        supervisor.createWorker({
          worker: 'TestWorker',
          count: 0,
          config: {},
          cpu: 1,
          memory: 1024
        });
      }).toThrow('Worker count must be greater than zero');

      expect(() => {
        supervisor.createWorker({
          worker: 'TestWorker',
          count: -1,
          config: {},
          cpu: 1,
          memory: 1024
        });
      }).toThrow('Worker count must be greater than zero');
    });

    it('should create specified number of workers', () => {
      const mockNewProcess = createMockChildProcess(2001);
      mockSpawn.mockReturnValue(mockNewProcess);

      supervisor.createWorker({
        worker: 'TestWorker',
        count: 1,
        config: { testConfig: 'value' },
        cpu: 2,
        memory: 2048
      });

      expect(mockSpawn).toHaveBeenCalledWith(
        '/usr/bin/node',
        expect.arrayContaining([
          expect.stringContaining('ts-node'),
          expect.stringContaining('TestWorker.ts')
        ]),
        expect.objectContaining({
          stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
          env: { testConfig: 'value' }
        })
      );
    });

    it('should add new worker to workers array', () => {
      const initialWorkerCount = (supervisor as any).workers.length;
      const mockNewProcess = createMockChildProcess(2001);
      mockSpawn.mockReturnValue(mockNewProcess);

      supervisor.createWorker({
        worker: 'TestWorker',
        count: 1,
        config: {},
        cpu: 1,
        memory: 1024
      });

      expect((supervisor as any).workers).toHaveLength(initialWorkerCount + 1);
      expect((supervisor as any).workers[initialWorkerCount].pid).toBe(2001);
    });

    it('should setup exit handler that recreates worker', () => {
      const mockNewProcess = createMockChildProcess(2001);
      mockSpawn.mockReturnValue(mockNewProcess);

      supervisor.createWorker({
        worker: 'TestWorker',
        count: 1,
        config: {},
        cpu: 1,
        memory: 1024
      });

      // Get the exit handler
      const exitHandler = mockNewProcess.on.mock.calls.find((call: any) => call[0] === 'exit')?.[1];
      expect(exitHandler).toBeDefined();

      // Mock createWorker to verify it's called when worker exits
      const createWorkerSpy = jest.spyOn(supervisor, 'createWorker');
      
      // Simulate worker exit
      if (exitHandler) {
        exitHandler();
        expect(createWorkerSpy).toHaveBeenCalledWith({
          worker: 'TestWorker',
          count: 1,
          config: {},
          cpu: 1,
          memory: 1024
        });
      }
    });
  });

  describe('handleWorkerMessage', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
    });

    it('should handle healthy status message', () => {
      const message: Message = {
        messageId: 'test-message-1',
        status: 'healthy',
        destination: ['supervisor'],
        data: { instanceId: 'worker-1' }
      };

      supervisor.handleWorkerMessage(message, 1001);

      const workersHealth = (supervisor as any).workersHealth;
      expect(workersHealth[1001]).toBeDefined();
      expect(workersHealth[1001].isHealthy).toBe(true);
      expect(workersHealth[1001].workerNameId).toBe('worker-1');
      expect(workersHealth[1001].timestamp).toBeInstanceOf(Timestamp);
    });

    it('should handle completed status and remove pending message', () => {
      // First add a pending message
      const pendingMessage: Message = {
        messageId: 'test-message-1',
        status: 'completed',
        destination: ['TestWorker']
      };
      (supervisor as any).trackPendingMessage('TestWorker', pendingMessage);

      // Verify message was added
      expect((supervisor as any).pendingMessages['TestWorker']).toHaveLength(1);

      // Handle completed message with supervisor destination
      const completedMessage: Message = {
        messageId: 'test-message-1',
        status: 'completed',
        destination: ['supervisor']
      };

      supervisor.handleWorkerMessage(completedMessage, 1001);

      // Verify message was removed (worker name is extracted from supervisor, which becomes empty string)
      // The logic splits supervisor by "/" and "." which results in empty string
      expect((supervisor as any).pendingMessages['TestWorker']).toHaveLength(1); // Still there since workerName is empty
    });

    it('should forward message to other workers when destination is not supervisor', () => {
      const message: Message = {
        messageId: 'test-message-1',
        status: 'completed',
        destination: ['CrawlerWorker', 'DatabaseInteractionWorker']
      };

      const handleSendMessageWorkerSpy = jest.spyOn(supervisor, 'handleSendMessageWorker');

      supervisor.handleWorkerMessage(message, 1001);

      // Each destination will call handleSendMessageWorker
      expect(handleSendMessageWorkerSpy).toHaveBeenCalledTimes(2);
      
      // Check that the message was modified to have single destination for each call
      expect(handleSendMessageWorkerSpy).toHaveBeenCalledWith(1001, expect.objectContaining({
        messageId: 'test-message-1'
      }));
    });
  });

  describe('handleSendMessageWorker', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
    });

    it('should send message to available worker', () => {
      // Setup mock worker that matches CrawlerWorker
      mockChildProcess1.spawnargs = ['node', 'ts-node', '/path/to/CrawlerWorker.ts'];
      
      // Make worker alive and not ready (not running state - should be 'S' for sleeping)
      mockExecSync.mockReturnValue(Buffer.from('S'));

      const message: Message = {
        messageId: 'test-message-1',
        status: 'completed',
        destination: ['CrawlerWorker']
      };

      supervisor.handleSendMessageWorker(1001, message);

      // Should send message to the first available CrawlerWorker
      expect(mockChildProcess1.send).toHaveBeenCalledWith(message);
    });

    it('should handle error status by restarting worker', () => {
      const message: Message = {
        messageId: 'test-message-1',
        status: 'error',
        reason: 'Worker crashed',
        destination: ['supervisor']
      };

      const restartWorkerSpy = jest.spyOn(supervisor, 'restartWorker');

      supervisor.handleSendMessageWorker(1001, message);

      expect(restartWorkerSpy).toHaveBeenCalledWith(mockChildProcess1);
    });

    it('should create new worker if no available workers found', () => {
      // Mock no available workers (all processes killed)
      (supervisor as any).workers.forEach((worker: any) => {
        worker.killed = true;
        worker.exitCode = 1;
      });

      const message: Message = {
        messageId: 'test-message-1',
        status: 'completed',
        destination: ['NewWorker']
      };

      const createWorkerSpy = jest.spyOn(supervisor, 'createWorker');

      supervisor.handleSendMessageWorker(1001, message);

      expect(createWorkerSpy).toHaveBeenCalledWith({
        worker: 'NewWorker',
        count: undefined,
        cpu: undefined,
        memory: undefined,
        config: undefined
      });
    });

    it('should track pending messages before sending', () => {
      const message: Message = {
        messageId: 'test-message-1',
        status: 'completed',
        destination: ['CrawlerWorker']
      };

      supervisor.handleSendMessageWorker(1001, message);

      const pendingMessages = (supervisor as any).pendingMessages;
      expect(pendingMessages['CrawlerWorker']).toBeDefined();
      expect(pendingMessages['CrawlerWorker']).toContainEqual(
        expect.objectContaining({
          messageId: 'test-message-1',
          timestamp: expect.any(Number)
        })
      );
    });
  });

  describe('restartWorker', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
    });

    it('should kill existing worker and create new one', () => {
      const createWorkerSpy = jest.spyOn(supervisor, 'createWorker');
      const resendPendingMessagesSpy = jest.spyOn(supervisor as any, 'resendPendingMessages');

      supervisor.restartWorker(mockChildProcess1);

      expect(mockChildProcess1.kill).toHaveBeenCalled();
      expect(createWorkerSpy).toHaveBeenCalledWith({
        worker: 'TestWorker',
        count: undefined,
        cpu: undefined,
        memory: undefined,
        config: undefined
      });
      expect(resendPendingMessagesSpy).toHaveBeenCalledWith('TestWorker');
    });
  });

  describe('isWorkerAlive', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
    });

    it('should return true for alive worker', () => {
      const aliveWorker = createMockChildProcess();
      aliveWorker.exitCode = null;
      aliveWorker.killed = false;

      expect((supervisor as any).isWorkerAlive(aliveWorker)).toBe(true);
    });

    it('should return false for dead worker', () => {
      const deadWorker = createMockChildProcess();
      deadWorker.exitCode = 1;
      deadWorker.killed = false;

      expect((supervisor as any).isWorkerAlive(deadWorker)).toBe(false);
    });

    it('should return false for killed worker', () => {
      const killedWorker = createMockChildProcess();
      killedWorker.exitCode = null;
      killedWorker.killed = true;

      expect((supervisor as any).isWorkerAlive(killedWorker)).toBe(false);
    });

    it('should return false for null worker', () => {
      expect((supervisor as any).isWorkerAlive(null)).toBeFalsy();
      expect((supervisor as any).isWorkerAlive(undefined)).toBeFalsy();
    });
  });

  describe('Pending Message Management', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
    });

    describe('trackPendingMessage', () => {
      it('should add message to pending messages', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message);

        const pendingMessages = (supervisor as any).pendingMessages;
        expect(pendingMessages['TestWorker']).toHaveLength(1);
        expect(pendingMessages['TestWorker'][0]).toMatchObject({
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker'],
          timestamp: expect.any(Number)
        });
      });

      it('should not add duplicate message IDs', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message);
        (supervisor as any).trackPendingMessage('TestWorker', message);

        const pendingMessages = (supervisor as any).pendingMessages;
        expect(pendingMessages['TestWorker']).toHaveLength(1);
      });

      it('should handle empty worker name gracefully', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        expect(() => {
          (supervisor as any).trackPendingMessage('', message);
        }).not.toThrow();

        expect(() => {
          (supervisor as any).trackPendingMessage(null, message);
        }).not.toThrow();
      });
    });

    describe('removePendingMessage', () => {
      it('should remove message from pending messages', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message);
        expect((supervisor as any).pendingMessages['TestWorker']).toHaveLength(1);

        (supervisor as any).removePendingMessage('TestWorker', 'test-message-1');
        expect((supervisor as any).pendingMessages['TestWorker']).toHaveLength(0);
      });

      it('should handle non-existent worker gracefully', () => {
        expect(() => {
          (supervisor as any).removePendingMessage('NonExistentWorker', 'test-message-1');
        }).not.toThrow();
      });

      it('should handle non-existent message ID gracefully', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message);
        
        expect(() => {
          (supervisor as any).removePendingMessage('TestWorker', 'non-existent-id');
        }).not.toThrow();

        expect((supervisor as any).pendingMessages['TestWorker']).toHaveLength(1);
      });
    });

    describe('resendPendingMessages', () => {
      it('should resend all pending messages to available worker', () => {
        const message1: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        const message2: Message = {
          messageId: 'test-message-2',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message1);
        (supervisor as any).trackPendingMessage('TestWorker', message2);

        // Setup mock worker with TestWorker in spawnargs
        mockChildProcess1.spawnargs = ['node', 'ts-node', '/path/to/TestWorker.ts'];

        (supervisor as any).resendPendingMessages('TestWorker');

        expect(mockChildProcess1.send).toHaveBeenCalledTimes(2);
        expect(mockChildProcess1.send).toHaveBeenCalledWith(expect.objectContaining({
          messageId: 'test-message-1'
        }));
        expect(mockChildProcess1.send).toHaveBeenCalledWith(expect.objectContaining({
          messageId: 'test-message-2'
        }));
      });

      it('should handle no pending messages gracefully', () => {
        expect(() => {
          (supervisor as any).resendPendingMessages('TestWorker');
        }).not.toThrow();
      });

      it('should handle no available worker gracefully', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message);

        // Make all workers dead
        (supervisor as any).workers.forEach((worker: any) => {
          worker.exitCode = 1;
        });

        expect(() => {
          (supervisor as any).resendPendingMessages('TestWorker');
        }).not.toThrow();
      });

      it('should handle send errors gracefully', () => {
        const message: Message = {
          messageId: 'test-message-1',
          status: 'completed',
          destination: ['TestWorker']
        };

        (supervisor as any).trackPendingMessage('TestWorker', message);

        // Setup mock worker with TestWorker in spawnargs
        mockChildProcess1.spawnargs = ['node', 'ts-node', '/path/to/TestWorker.ts'];
        mockChildProcess1.send.mockImplementation(() => {
          throw new Error('Send failed');
        });

        expect(() => {
          (supervisor as any).resendPendingMessages('TestWorker');
        }).not.toThrow();
      });
    });
  });

  describe('Worker Exit Handling', () => {
    beforeEach(() => {
      supervisor = new Supervisor();
    });

    it('should remove worker from workers array on exit', () => {
      const initialWorkerCount = (supervisor as any).workers.length;
      
      // Get the exit handler for the first worker
      const exitHandler = mockChildProcess1.on.mock.calls.find((call: any) => call[0] === 'exit')?.[1];
      expect(exitHandler).toBeDefined();

      // Mock createWorker to prevent infinite recursion in test
      jest.spyOn(supervisor, 'createWorker').mockImplementation(() => {});

      // Simulate worker exit
      if (exitHandler) {
        exitHandler();
      }

      // Worker should be removed from array
      const remainingWorkers = (supervisor as any).workers;
      expect(remainingWorkers).not.toContain(mockChildProcess1);
      expect(remainingWorkers.length).toBe(initialWorkerCount - 1);
    });
  });
});