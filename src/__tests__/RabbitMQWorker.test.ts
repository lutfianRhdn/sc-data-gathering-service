import { RabbitMQWorker } from '../workers/RabbitMQWorker';
import { Message } from '../utils/handleMessage';
import * as amqp from 'amqplib';
import { v4 as uuidv4 } from 'uuid';

// Mock external dependencies
jest.mock('amqplib');
jest.mock('../utils/handleMessage', () => ({
  sendMessagetoSupervisor: jest.fn(),
  Message: jest.fn()
}));
jest.mock('../utils/log');
jest.mock('uuid', () => ({
  v4: jest.fn(() => 'mocked-uuid-123')
}));
jest.mock('../configs/env', () => ({
  RABBITMQ_URL: 'amqp://admin:admin123@localhost:5672/test'
}));

const mockAmqp = amqp as jest.Mocked<typeof amqp>;
const mockSendMessagetoSupervisor = require('../utils/handleMessage').sendMessagetoSupervisor;

describe('RabbitMQWorker', () => {
  let worker: RabbitMQWorker;
  let mockConnection: jest.Mocked<amqp.Connection>;
  let mockChannel: jest.Mocked<amqp.Channel>;
  let mockConsumeChannel: jest.Mocked<amqp.Channel>;
  let mockProduceChannel: jest.Mocked<amqp.Channel>;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Setup RabbitMQ mocks
    mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: 'test-queue' }),
      consume: jest.fn(),
      sendToQueue: jest.fn()
    } as any;

    mockConsumeChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: 'consume-queue' }),
      consume: jest.fn()
    } as any;

    mockProduceChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: 'produce-queue' }),
      sendToQueue: jest.fn()
    } as any;

    mockConnection = {
      createChannel: jest.fn()
        .mockResolvedValueOnce(mockConsumeChannel)
        .mockResolvedValueOnce(mockProduceChannel),
      on: jest.fn(),
      close: jest.fn()
    } as any as amqp.Connection;

    mockAmqp.connect.mockResolvedValue(mockConnection);

    // Mock process.on to prevent actual event listener registration
    jest.spyOn(process, 'on').mockImplementation(() => process);
    
    // Mock setInterval for healthCheck
    jest.spyOn(global, 'setInterval').mockImplementation((callback: Function) => {
      return 'mock-timer' as any;
    });

    // Setup environment variables
    process.env.consumeQueue = 'test-consume-queue';
    process.env.consumeCompensationQueue = 'test-consume-compensation-queue';
    process.env.dataGatheringCompensationQueue = 'test-produce-compensation-queue';
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Constructor', () => {
    it('should create instance with correct instanceId format', () => {
      worker = new RabbitMQWorker();
      
      expect(worker.getInstanceId()).toBe('RabbitMqWorker-mocked-uuid-123');
      expect(worker.isBusy).toBe(false);
    });

    it('should set connection string from env or default', () => {
      worker = new RabbitMQWorker();
      
      expect(worker['string_connection']).toBe('amqp://admin:admin123@localhost:5672/test');
    });

    it('should use default connection string when env not set', () => {
      // Temporarily remove env variable
      const originalEnv = process.env.RABBITMQ_URL;
      delete process.env.RABBITMQ_URL;
      
      const mockAmqpForDefault = require('amqplib');
      jest.doMock('../configs/env', () => ({
        RABBITMQ_URL: undefined
      }));

      worker = new RabbitMQWorker();
      
      expect(worker['string_connection']).toBe('amqp://admin:admin123@70.153.61.68:5672/dev');
      
      // Restore env variable
      if (originalEnv) process.env.RABBITMQ_URL = originalEnv;
    });

    it('should handle constructor errors gracefully', () => {
      const mockConsoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
      
      // Mock run method to throw error
      const originalRun = RabbitMQWorker.prototype.run;
      RabbitMQWorker.prototype.run = jest.fn().mockRejectedValue(new Error('Test error'));
      
      worker = new RabbitMQWorker();
      
      // Restore original method
      RabbitMQWorker.prototype.run = originalRun;
      mockConsoleError.mockRestore();
    });
  });

  describe('run', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should connect to RabbitMQ successfully', async () => {
      await worker.run();
      
      expect(mockAmqp.connect).toHaveBeenCalledWith(
        'amqp://admin:admin123@localhost:5672/test',
        {
          heartbeat: 60,
          timeout: 10000
        }
      );
    });

    it('should setup connection event listeners', async () => {
      await worker.run();
      
      expect(mockConnection.on).toHaveBeenCalledWith('error', expect.any(Function));
      expect(mockConnection.on).toHaveBeenCalledWith('close', expect.any(Function));
      expect(mockConnection.on).toHaveBeenCalledWith('blocked', expect.any(Function));
    });

    it('should handle connection errors', async () => {
      await worker.run();
      
      // Get the error handler
      const errorHandler = mockConnection.on.mock.calls.find(
        call => call[0] === 'error'
      )[1];

      const testError = new Error('Connection error');
      errorHandler(testError);

      // Should log the error (mocked)
      expect(mockConnection.on).toHaveBeenCalledWith('error', expect.any(Function));
    });

    it('should handle connection close', async () => {
      await worker.run();
      
      // Get the close handler
      const closeHandler = mockConnection.on.mock.calls.find(
        call => call[0] === 'close'
      )[1];

      const closeReason = new Error('Connection closed');
      closeHandler(closeReason);

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'mocked-uuid-123',
        status: 'error',
        reason: 'Connection closed',
        data: []
      });
    });

    it('should handle connection blocked', async () => {
      await worker.run();
      
      // Get the blocked handler
      const blockedHandler = mockConnection.on.mock.calls.find(
        call => call[0] === 'blocked'
      )[1];

      blockedHandler('Memory alarm');

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'mocked-uuid-123',
        status: 'error',
        data: [],
        reason: 'Memory alarm'
      });
    });

    it('should start health check', async () => {
      await worker.run();
      
      expect(setInterval).toHaveBeenCalledWith(expect.any(Function), 10000);
    });

    it('should start listening for tasks', async () => {
      const listenTaskSpy = jest.spyOn(worker, 'listenTask').mockResolvedValue();
      
      await worker.run();
      
      expect(listenTaskSpy).toHaveBeenCalled();
    });

    it('should start consuming messages', async () => {
      const consumeMessageSpy = jest.spyOn(worker, 'consumeMessage').mockResolvedValue();
      
      await worker.run();
      
      expect(consumeMessageSpy).toHaveBeenCalledWith('test-consume-queue');
    });

    it('should handle connection failure', async () => {
      mockAmqp.connect.mockRejectedValue(new Error('Connection failed'));
      
      await expect(worker.run()).rejects.toThrow('Connection failed');
    });

    it('should throw error when no connection string provided', async () => {
      worker['string_connection'] = '';
      
      await expect(worker.run()).rejects.toThrow('Connection string is not provided');
    });
  });

  describe('healthCheck', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should setup health check interval', () => {
      worker.healthCheck();

      expect(setInterval).toHaveBeenCalledWith(expect.any(Function), 10000);
    });

    it('should send health status to supervisor', () => {
      worker.healthCheck();

      // Get the callback function from setInterval
      const healthCallback = (setInterval as jest.Mock).mock.calls[0][0];
      healthCallback();

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'mocked-uuid-123',
        status: 'healthy',
        data: {
          instanceId: 'RabbitMqWorker-mocked-uuid-123',
          timestamp: expect.any(String)
        }
      });
    });
  });

  describe('consumeMessage', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should create channel and assert queue', async () => {
      // Setup connection
      await worker.run();
      
      await worker.consumeMessage('test-queue');
      
      expect(mockConnection.createChannel).toHaveBeenCalled();
      expect(mockConsumeChannel.assertQueue).toHaveBeenCalledWith('test-queue', {
        durable: true
      });
    });

    it('should setup message consumer', async () => {
      await worker.run();
      
      await worker.consumeMessage('test-queue');
      
      expect(mockConsumeChannel.consume).toHaveBeenCalledWith(
        'test-queue',
        expect.any(Function),
        { noAck: true }
      );
    });

    it('should handle consumed messages from main queue', async () => {
      await worker.run();
      await worker.consumeMessage('test-consume-queue');
      
      // Get the consume callback
      const consumeCallback = mockConsumeChannel.consume.mock.calls[0][1];
      
      const testMessage = {
        content: Buffer.from(JSON.stringify({ projectId: 'test123', keyword: 'test' })),
        fields: {} as any,
        properties: {} as any
      };

      consumeCallback(testMessage);

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: { projectId: 'test123', keyword: 'test' },
        destination: ['CrawlerWorker/crawling']
      });
    });

    it('should handle consumed messages from compensation queue', async () => {
      await worker.run();
      await worker.consumeMessage('test-consume-compensation-queue');
      
      // Get the consume callback
      const consumeCallback = mockConsumeChannel.consume.mock.calls[0][1];
      
      const testMessage = {
        content: Buffer.from(JSON.stringify({ projectId: 'test123', error: 'rollback' })),
        fields: {} as any,
        properties: {} as any
      };

      consumeCallback(testMessage);

      // Should not send message to supervisor for compensation queue
      expect(mockSendMessagetoSupervisor).not.toHaveBeenCalled();
    });

    it('should handle null messages', async () => {
      await worker.run();
      await worker.consumeMessage('test-queue');
      
      // Get the consume callback
      const consumeCallback = mockConsumeChannel.consume.mock.calls[0][1];
      
      // Test with null message
      expect(() => consumeCallback(null)).not.toThrow();
    });

    it('should throw error when connection not established', async () => {
      // Don't call run() to establish connection
      await expect(worker.consumeMessage('test-queue')).rejects.toThrow('Connection is not established');
    });
  });

  describe('produceMessage', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should create channel and send message to default queue', async () => {
      await worker.run();
      
      const testMessage = { projectId: 'test123', data: 'test data' };
      
      await worker.produceMessage(testMessage);
      
      expect(mockConnection.createChannel).toHaveBeenCalled();
      expect(mockProduceChannel.assertQueue).toHaveBeenCalledWith('dataGatheringQueue', {
        durable: true
      });
      expect(mockProduceChannel.sendToQueue).toHaveBeenCalledWith(
        'dataGatheringQueue',
        Buffer.from(JSON.stringify(testMessage)),
        { persistent: true }
      );
    });

    it('should send message to specified queue', async () => {
      await worker.run();
      
      const testMessage = { projectId: 'test123', data: 'test data' };
      const customQueue = 'custom-queue';
      
      await worker.produceMessage(testMessage, customQueue);
      
      expect(mockProduceChannel.assertQueue).toHaveBeenCalledWith(customQueue, {
        durable: true
      });
      expect(mockProduceChannel.sendToQueue).toHaveBeenCalledWith(
        customQueue,
        Buffer.from(JSON.stringify(testMessage)),
        { persistent: true }
      );
    });

    it('should handle channel creation errors', async () => {
      await worker.run();
      
      const channelError = new Error('Channel creation failed');
      mockConnection.createChannel.mockRejectedValueOnce(channelError);
      
      const testMessage = { test: 'data' };
      
      // Should not throw, but handle error internally
      await expect(worker.produceMessage(testMessage)).resolves.toBeUndefined();
    });

    it('should handle send message errors', async () => {
      await worker.run();
      
      mockProduceChannel.sendToQueue.mockImplementation(() => {
        throw new Error('Send failed');
      });
      
      const testMessage = { test: 'data' };
      
      // Should not throw, but handle error internally
      await expect(worker.produceMessage(testMessage)).resolves.toBeUndefined();
    });

    it('should handle null channel error', async () => {
      await worker.run();
      
      // Force channel to be null after creation
      worker['produceChannel'] = null as any;
      
      const testMessage = { test: 'data' };
      
      await expect(worker.produceMessage(testMessage)).resolves.toBeUndefined();
    });
  });

  describe('listenTask', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should setup process message listener', async () => {
      await worker.listenTask();

      expect(process.on).toHaveBeenCalledWith('message', expect.any(Function));
    });

    it('should handle failed message with NO_TWEET_FOUND reason', async () => {
      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();
      
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'failed',
        reason: 'NO_TWEET_FOUND',
        data: { projectId: 'test123' }
      };

      messageHandler(testMessage);

      expect(produceMessageSpy).toHaveBeenCalledWith(
        { projectId: 'test123' },
        'test-produce-compensation-queue'
      );
    });

    it('should handle successful message', async () => {
      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();
      
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: { projectId: 'test123', tweets: [] }
      };

      messageHandler(testMessage);

      expect(produceMessageSpy).toHaveBeenCalledWith(
        { projectId: 'test123', tweets: [] },
        'dataGatheringQueue'
      );
    });

    it('should handle produce message errors gracefully', async () => {
      jest.spyOn(worker, 'produceMessage').mockRejectedValue(new Error('Produce failed'));
      
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: { test: 'data' }
      };

      // Should not throw
      expect(() => messageHandler(testMessage)).not.toThrow();
    });

    it('should handle listen task errors', async () => {
      // Force an error by making process.on throw
      jest.spyOn(process, 'on').mockImplementation(() => {
        throw new Error('Process listener error');
      });

      await expect(worker.listenTask()).rejects.toThrow('Process listener error');
    });
  });

  describe('getInstanceId', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should return correct instance ID format', () => {
      const instanceId = worker.getInstanceId();
      
      expect(instanceId).toBe('RabbitMqWorker-mocked-uuid-123');
    });

    it('should return consistent instance ID', () => {
      const firstCall = worker.getInstanceId();
      const secondCall = worker.getInstanceId();
      
      expect(firstCall).toBe(secondCall);
    });
  });

  describe('Connection Management', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should handle connection establishment properly', async () => {
      await worker.run();
      
      expect(worker['connection']).toBe(mockConnection);
      expect(mockAmqp.connect).toHaveBeenCalledWith(
        'amqp://admin:admin123@localhost:5672/test',
        {
          heartbeat: 60,
          timeout: 10000
        }
      );
    });

    it('should handle null connection in consumeMessage', async () => {
      // Don't call run() to establish connection
      worker['connection'] = null;
      
      await expect(worker.consumeMessage('test-queue')).rejects.toThrow('Connection is not established');
    });
  });

  describe('Integration Tests', () => {
    beforeEach(() => {
      worker = new RabbitMQWorker();
    });

    it('should handle complete message flow from consumption to production', async () => {
      await worker.run();
      
      // Setup consume message
      await worker.consumeMessage('test-consume-queue');
      
      // Get the consume callback
      const consumeCallback = mockConsumeChannel.consume.mock.calls[0][1];
      
      // Simulate message consumption
      const incomingMessage = {
        content: Buffer.from(JSON.stringify({ 
          projectId: 'test123',
          keyword: 'test keyword',
          tweetToken: 'token123'
        })),
        fields: {} as any,
        properties: {} as any
      };

      consumeCallback(incomingMessage);

      // Should send message to CrawlerWorker
      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: {
          projectId: 'test123',
          keyword: 'test keyword',
          tweetToken: 'token123'
        },
        destination: ['CrawlerWorker/crawling']
      });
    });

    it('should handle complete message flow from task listener to production', async () => {
      await worker.run();
      await worker.listenTask();

      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();

      // Get the message handler
      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      // Simulate completed crawling result
      const completedMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          projectId: 'test123',
          tweets: [
            { id: '1', text: 'tweet 1' },
            { id: '2', text: 'tweet 2' }
          ]
        }
      };

      messageHandler(completedMessage);

      expect(produceMessageSpy).toHaveBeenCalledWith(
        {
          projectId: 'test123',
          tweets: [
            { id: '1', text: 'tweet 1' },
            { id: '2', text: 'tweet 2' }
          ]
        },
        'dataGatheringQueue'
      );
    });

    it('should handle error scenarios in complete workflow', async () => {
      await worker.run();
      await worker.listenTask();

      const produceMessageSpy = jest.spyOn(worker, 'produceMessage').mockResolvedValue();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      // Simulate failed crawling with NO_TWEET_FOUND
      const failedMessage: Message = {
        messageId: 'test-1',
        status: 'failed',
        reason: 'NO_TWEET_FOUND',
        data: { projectId: 'test123', keyword: 'nonexistent' }
      };

      messageHandler(failedMessage);

      // Should send to compensation queue
      expect(produceMessageSpy).toHaveBeenCalledWith(
        { projectId: 'test123', keyword: 'nonexistent' },
        'test-produce-compensation-queue'
      );
    });
  });

  describe('Environment Configuration', () => {
    it('should use environment variables for queue names', () => {
      worker = new RabbitMQWorker();
      
      expect(worker['consumeQueue']).toBe('test-consume-queue');
      expect(worker['consumeCompensationQueue']).toBe('test-consume-compensation-queue');
      expect(worker['produceCompensationQueue']).toBe('test-produce-compensation-queue');
    });

    it('should use default produce queue name', () => {
      worker = new RabbitMQWorker();
      
      expect(worker['produceQueue']).toBe('dataGatheringQueue');
    });
  });
});