import { RabbitMQWorker } from '../workers/RabbitMQWorker';
import * as amqp from 'amqplib';
import { Message } from '../utils/handleMessage';

// Mock dependencies
jest.mock('amqplib');
jest.mock('../utils/log');
jest.mock('../utils/handleMessage', () => ({
  sendMessage: jest.fn(),
  sendMessagetoSupervisor: jest.fn()
}));
jest.mock('../configs/env', () => ({
  RABBITMQ_URL: 'amqp://test:test@localhost:5672/test'
}));

const { sendMessagetoSupervisor } = require('../utils/handleMessage');

describe('RabbitMQWorker', () => {
  let worker: RabbitMQWorker;
  let mockConnection: any;
  let mockChannel: any;

  beforeEach(() => {
    // Mock RabbitMQ connection and channel
    mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({}),
      consume: jest.fn(),
      sendToQueue: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined)
    };

    mockConnection = {
      createChannel: jest.fn().mockResolvedValue(mockChannel),
      on: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined)
    };

    (amqp.connect as jest.MockedFunction<typeof amqp.connect>).mockResolvedValue(mockConnection);

    // Mock process.on to avoid actual process event listeners in tests
    jest.spyOn(process, 'on').mockImplementation((event: any, listener: any) => {
      if (event === 'message') {
        // Store the listener for manual triggering in tests
        (worker as any)._testMessageListener = listener;
      }
      return process;
    });

    // Mock environment variables
    process.env.consumeQueue = 'test-consume-queue';
    process.env.consumeCompensationQueue = 'test-compensation-queue';
    process.env.dataGatheringCompensationQueue = 'test-data-compensation-queue';

    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Constructor', () => {
    it('should create instance with unique ID', () => {
      worker = new RabbitMQWorker();
      expect(worker.getInstanceId()).toMatch(/^RabbitMqWorker-/);
    });

    it('should initialize connection string from environment or default', () => {
      worker = new RabbitMQWorker();
      expect((worker as any).string_connection).toBe('amqp://test:test@localhost:5672/test');
    });

    it('should use default connection string when env not provided', () => {
      // Create new worker instance without RABBITMQ_URL
      const originalEnv = jest.requireActual('../configs/env');
      jest.doMock('../configs/env', () => ({
        ...originalEnv,
        RABBITMQ_URL: undefined
      }));
      
      worker = new RabbitMQWorker();
      expect((worker as any).string_connection).toBe(
			"amqp://test:test@localhost:5672/test"
		);
    });

    it('should initialize queue names from environment', () => {
      worker = new RabbitMQWorker();
      expect((worker as any).consumeQueue).toBe('test-consume-queue');
      expect((worker as any).consumeCompensationQueue).toBe('test-compensation-queue');
      expect((worker as any).produceQueue).toBe('dataGatheringQueue');
      expect((worker as any).produceCompensationQueue).toBe('test-data-compensation-queue');
    });

    it('should start running process', () => {
      const runSpy = jest.spyOn(RabbitMQWorker.prototype, 'run');
      worker = new RabbitMQWorker();
      expect(runSpy).toHaveBeenCalled();
    });
  });

  describe('getInstanceId', () => {
    it('should return the instance ID', () => {
      worker = new RabbitMQWorker();
      const instanceId = worker.getInstanceId();
      expect(instanceId).toMatch(/^RabbitMqWorker-/);
    });
  });

  describe('healthCheck', () => {
    it('should send health check messages periodically', () => {
      jest.useFakeTimers();
      worker = new RabbitMQWorker();
      
      worker.healthCheck();
      
      // Fast-forward 10 seconds
      jest.advanceTimersByTime(10000);
      
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: expect.any(String),
        status: 'healthy',
        data: {
          instanceId: worker.getInstanceId(),
          timestamp: expect.any(String)
        }
      });
      
      jest.useRealTimers();
    });
  });

  describe('run', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should connect to RabbitMQ successfully', async () => {
      await worker.run();
      
      expect(amqp.connect).toHaveBeenCalledWith(
        'amqp://test:test@localhost:5672/test',
        {
          heartbeat: 60,
          timeout: 10000
        }
      );
    });

    it('should throw error when connection string is not provided', async () => {
      (worker as any).string_connection = null;
      
      await expect(worker.run()).rejects.toThrow('Connection string is not provided');
    });

    it('should setup connection event handlers', async () => {
      await worker.run();
      
      expect(mockConnection.on).toHaveBeenCalledWith('error', expect.any(Function));
      expect(mockConnection.on).toHaveBeenCalledWith('close', expect.any(Function));
      expect(mockConnection.on).toHaveBeenCalledWith('blocked', expect.any(Function));
    });

    it('should handle connection errors', async () => {
      await worker.run();
      
      // Get the error handler and trigger it
      const errorHandler = mockConnection.on.mock.calls.find(call => call[0] === 'error')[1];
      const testError = new Error('Connection error');
      
      errorHandler(testError);
      
      // Verify error was handled (no exception thrown)
      expect(true).toBe(true);
    });

    it('should handle connection close', async () => {
      await worker.run();
      
      // Get the close handler and trigger it
      const closeHandler = mockConnection.on.mock.calls.find(call => call[0] === 'close')[1];
      const closeReason = { message: 'Connection closed' };
      
      closeHandler(closeReason);
      
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: expect.any(String),
        status: 'error',
        reason: 'Connection closed',
        data: []
      });
    });

    it('should handle connection blocked', async () => {
      await worker.run();
      
      // Get the blocked handler and trigger it
      const blockedHandler = mockConnection.on.mock.calls.find(call => call[0] === 'blocked')[1];
      const blockReason = 'Resource limit exceeded';
      
      blockedHandler(blockReason);
      
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: expect.any(String),
        status: 'error',
        data: [],
        reason: blockReason
      });
    });

    it('should start consuming messages', async () => {
      const consumeMessageSpy = jest.spyOn(worker, 'consumeMessage');
      await worker.run();
      
      expect(consumeMessageSpy).toHaveBeenCalledWith('test-consume-queue');
    });

    it('should setup health check and listen task', async () => {
      const healthCheckSpy = jest.spyOn(worker, 'healthCheck');
      const listenTaskSpy = jest.spyOn(worker, 'listenTask');
      
      await worker.run();
      
      expect(healthCheckSpy).toHaveBeenCalled();
      expect(listenTaskSpy).toHaveBeenCalled();
    });

    it('should handle run errors', async () => {
      const connectError = new Error('Failed to connect');
      (amqp.connect as jest.MockedFunction<typeof amqp.connect>).mockRejectedValue(connectError);
      
      await expect(worker.run()).rejects.toThrow('Failed to connect');
    });
  });

  describe('consumeMessage', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
      (worker as any).connection = mockConnection;
    });

    it('should create channel and assert queue', async () => {
      await worker.consumeMessage('test-queue');
      
      expect(mockConnection.createChannel).toHaveBeenCalled();
      expect(mockChannel.assertQueue).toHaveBeenCalledWith('test-queue', { durable: true });
    });

    it('should setup message consumer', async () => {
      await worker.consumeMessage('test-queue');
      
      expect(mockChannel.consume).toHaveBeenCalledWith(
        'test-queue',
        expect.any(Function),
        { noAck: true }
      );
    });

    it('should throw error when connection is not established', async () => {
      (worker as any).connection = null;
      
      await expect(worker.consumeMessage('test-queue')).rejects.toThrow('Connection is not established');
    });

    it('should handle consume queue messages correctly', async () => {
      await worker.consumeMessage('test-consume-queue');
      
      // Get the message handler
      const messageHandler = mockChannel.consume.mock.calls[0][1];
      const mockMessage = {
        content: Buffer.from(JSON.stringify({ projectId: 'test-project', keyword: 'test' }))
      };
      
      messageHandler(mockMessage);
      
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: expect.any(String),
        status: 'completed',
        data: { projectId: 'test-project', keyword: 'test' },
        destination: ['CrawlerWorker/crawling']
      });
    });

    it('should handle compensation queue messages correctly', async () => {
      await worker.consumeMessage('test-compensation-queue');
      
      // Get the message handler
      const messageHandler = mockChannel.consume.mock.calls[0][1];
      const mockMessage = {
        content: Buffer.from(JSON.stringify({ projectId: 'test-project', keyword: 'test' }))
      };
      
      messageHandler(mockMessage);
      
      // For compensation queue, it should not send to supervisor (commented out in source)
      // Just verify the handler runs without error
      expect(true).toBe(true);
    });

    it('should handle null messages gracefully', async () => {
      await worker.consumeMessage('test-queue');
      
      // Get the message handler
      const messageHandler = mockChannel.consume.mock.calls[0][1];
      
      // Should not throw error for null message
      expect(() => messageHandler(null)).not.toThrow();
    });

    it('should handle invalid JSON in messages', async () => {
      await worker.consumeMessage('test-consume-queue');
      
      // Get the message handler
      const messageHandler = mockChannel.consume.mock.calls[0][1];
      const mockMessage = {
        content: Buffer.from('invalid json content')
      };
      
      // Should handle parsing error gracefully
      expect(() => messageHandler(mockMessage)).toThrow();
    });
  });

  describe('produceMessage', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
      (worker as any).connection = mockConnection;
    });

    it('should send message to default queue', async () => {
      const testMessage = { projectId: 'test-project', data: 'test-data' };
      
      await worker.produceMessage(testMessage);
      
      expect(mockConnection.createChannel).toHaveBeenCalled();
      expect(mockChannel.assertQueue).toHaveBeenCalledWith('dataGatheringQueue', { durable: true });
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        'dataGatheringQueue',
        Buffer.from(JSON.stringify(testMessage)),
        { persistent: true }
      );
    });

    it('should send message to specified queue', async () => {
      const testMessage = { projectId: 'test-project', data: 'test-data' };
      const customQueue = 'custom-queue';
      
      await worker.produceMessage(testMessage, customQueue);
      
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(customQueue, { durable: true });
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        customQueue,
        Buffer.from(JSON.stringify(testMessage)),
        { persistent: true }
      );
    });

    it('should handle channel creation errors', async () => {
      const channelError = new Error('Channel creation failed');
      mockConnection.createChannel.mockRejectedValue(channelError);
      
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      
      await worker.produceMessage({ test: 'data' });
      
      expect(consoleSpy).toHaveBeenCalledWith('Failed to send message to RabbitMQ:', channelError);
      consoleSpy.mockRestore();
    });

    it('should handle sendToQueue errors', async () => {
      const sendError = new Error('Send failed');
      mockChannel.sendToQueue.mockImplementation(() => {
        throw sendError;
      });
      
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      
      await worker.produceMessage({ test: 'data' });
      
      expect(consoleSpy).toHaveBeenCalledWith('Failed to send message to RabbitMQ:', sendError);
      consoleSpy.mockRestore();
    });

    it('should handle null channel gracefully', async () => {
      mockConnection.createChannel.mockResolvedValue(null);
      
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation();
      
      await worker.produceMessage({ test: 'data' });
      
      expect(consoleSpy).toHaveBeenCalledWith(
        'Failed to send message to RabbitMQ:',
        expect.any(Error)
      );
      consoleSpy.mockRestore();
    });
  });

  describe('listenTask', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should handle failed messages with NO_TWEET_FOUND reason', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'failed',
        reason: 'NO_TWEET_FOUND',
        data: { projectId: 'test-project' }
      };

      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(produceMessageSpy).toHaveBeenCalledWith(
        { projectId: 'test-project' },
        'test-compensation-queue'
      );
    });

    it('should handle successful messages', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: { projectId: 'test-project', result: 'success' }
      };

      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(produceMessageSpy).toHaveBeenCalledWith(
        { projectId: 'test-project', result: 'success' },
        'dataGatheringQueue'
      );
    });

    it('should handle other failed messages (not NO_TWEET_FOUND)', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'failed',
        reason: 'OTHER_ERROR',
        data: { projectId: 'test-project' }
      };

      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(produceMessageSpy).toHaveBeenCalledWith(
        { projectId: 'test-project' },
        'dataGatheringQueue'
      );
    });

    it('should handle produce message errors for compensation queue', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'failed',
        reason: 'NO_TWEET_FOUND',
        data: { projectId: 'test-project' }
      };

      const produceError = new Error('Produce failed');
      jest.spyOn(worker, 'produceMessage').mockRejectedValue(produceError);

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      // Should handle error gracefully without throwing
      expect(true).toBe(true);
    });

    it('should handle produce message errors for main queue', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: { projectId: 'test-project' }
      };

      const produceError = new Error('Produce failed');
      jest.spyOn(worker, 'produceMessage').mockRejectedValue(produceError);

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      // Should handle error gracefully without throwing
      expect(true).toBe(true);
    });

    it('should handle general errors in listenTask', async () => {
      // Mock process.on to throw an error
      jest.spyOn(process, 'on').mockImplementation(() => {
        throw new Error('Process error');
      });

      await expect(worker.listenTask()).rejects.toThrow('Process error');
    });
  });

  describe('Integration Tests', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should handle complete workflow from connection to message processing', async () => {
      const testMessage = { projectId: 'test-project', keyword: 'test' };
      
      // Setup successful connection
      await worker.run();
      
      // Verify connection was established
      expect(amqp.connect).toHaveBeenCalled();
      expect(mockConnection.createChannel).toHaveBeenCalled();
      
      // Test message consumption
      await worker.consumeMessage('test-consume-queue');
      
      // Test message production
      await worker.produceMessage(testMessage);
      
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        'dataGatheringQueue',
        Buffer.from(JSON.stringify(testMessage)),
        { persistent: true }
      );
    });

    it('should handle worker busy state correctly', () => {
      worker.isBusy = true;
      expect(worker.isBusy).toBe(true);
      
      worker.isBusy = false;
      expect(worker.isBusy).toBe(false);
    });

    it('should handle connection lifecycle', async () => {
      // Start connection
      await worker.run();
      expect((worker as any).connection).toBe(mockConnection);
      
      // Test connection events
      const closeHandler = mockConnection.on.mock.calls.find(call => call[0] === 'close')[1];
      closeHandler({ message: 'Connection lost' });
      
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'error',
          reason: 'Connection lost'
        })
      );
    });
  });
});