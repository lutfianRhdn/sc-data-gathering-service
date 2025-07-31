// Mock external dependencies first before importing
jest.mock('mongodb', () => {
  const mockCollection = {
    insertMany: jest.fn(),
    aggregate: jest.fn().mockReturnValue({
      toArray: jest.fn()
    })
  };

  const mockDb = {
    collection: jest.fn().mockReturnValue(mockCollection)
  };

  const mockClient = {
    connect: jest.fn().mockResolvedValue(undefined),
    db: jest.fn().mockReturnValue(mockDb)
  };

  return {
    MongoClient: jest.fn().mockImplementation(() => mockClient),
    ObjectId: jest.fn().mockImplementation(() => ({ toString: () => 'mock-object-id' }))
  };
});

jest.mock('../utils/log');
jest.mock('../utils/handleMessage', () => ({
  sendMessagetoSupervisor: jest.fn(),
  Message: jest.fn()
}));
jest.mock('../configs/env', () => ({
  DATABASE_URL: 'mongodb://localhost:27017',
  DATABASE_NAME: 'test_db',
  DATABASE_COLLECTION: 'test_collection'
}));

// Mock process.on globally to prevent the auto-instantiated worker from setting up listeners
const originalProcessOn = process.on;
jest.spyOn(process, 'on').mockImplementation((event, listener) => {
  // Don't actually set up event listeners for the auto-instantiated worker
  return process;
});

import DatabaseInteractionWorker from '../workers/DatabaseInteractionWorker';
import * as mongoDB from 'mongodb';
import { Message } from '../utils/handleMessage';
import { ObjectId } from 'mongodb';

// Mock console.log to prevent test output pollution
const mockConsoleLog = jest.spyOn(console, 'log').mockImplementation(() => {});

const mockSendMessagetoSupervisor = require('../utils/handleMessage').sendMessagetoSupervisor;

describe('DatabaseInteractionWorker', () => {
  let worker: DatabaseInteractionWorker;
  let mockClient: jest.Mocked<mongoDB.MongoClient>;
  let mockDb: jest.Mocked<mongoDB.Db>;
  let mockCollection: jest.Mocked<mongoDB.Collection>;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Restore the original process.on for our tests
    process.on = originalProcessOn;
    
    // Setup MongoDB mocks for each test
    mockCollection = {
      insertMany: jest.fn(),
      aggregate: jest.fn().mockReturnValue({
        toArray: jest.fn()
      })
    } as any;

    mockDb = {
      collection: jest.fn().mockReturnValue(mockCollection)
    } as any;

    mockClient = {
      connect: jest.fn().mockResolvedValue(undefined),
      db: jest.fn().mockReturnValue(mockDb)
    } as any;

    (mongoDB.MongoClient as unknown as jest.Mock).mockImplementation(() => mockClient);

    // Mock process.on to prevent actual event listener registration during tests
    jest.spyOn(process, 'on').mockImplementation(() => process);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Constructor', () => {
    it('should create instance with correct instanceId format', () => {
      worker = new DatabaseInteractionWorker();
      
      expect(worker.getInstanceId()).toMatch(/^DatabaseInteractionWorker-\d+$/);
      expect(worker.isBusy).toBe(false);
    });

    it('should initialize MongoDB client and database connection', () => {
      worker = new DatabaseInteractionWorker();
      
      expect(mongoDB.MongoClient).toHaveBeenCalledWith('mongodb://localhost:27017');
      expect(mockClient.db).toHaveBeenCalledWith('test_db');
      expect(mockDb.collection).toHaveBeenCalledWith('test_collection');
    });

    it('should handle constructor errors gracefully', () => {
      const mockConsoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
      
      // Mock run method to throw error
      const originalRun = DatabaseInteractionWorker.prototype.run;
      DatabaseInteractionWorker.prototype.run = jest.fn().mockRejectedValue(new Error('Test error'));
      
      worker = new DatabaseInteractionWorker();
      
      // Restore original method
      DatabaseInteractionWorker.prototype.run = originalRun;
      mockConsoleError.mockRestore();
    });
  });

  describe('run', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should connect to MongoDB successfully', async () => {
      mockClient.connect.mockResolvedValue(undefined);
      
      await worker.run();
      
      expect(mockClient.connect).toHaveBeenCalled();
    });

    it('should handle MongoDB connection error', async () => {
      const connectionError = new Error('Connection failed');
      mockClient.connect.mockRejectedValue(connectionError);
      
      await worker.run();
      
      expect(mockClient.connect).toHaveBeenCalled();
    });

    it('should setup listenTask after connection', async () => {
      const listenTaskSpy = jest.spyOn(worker, 'listenTask').mockResolvedValue();
      
      await worker.run();
      
      expect(listenTaskSpy).toHaveBeenCalled();
    });

    it('should handle run method errors', async () => {
      const mockConsoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
      jest.spyOn(worker, 'listenTask').mockRejectedValue(new Error('Listen error'));
      
      await worker.run();
      
      mockConsoleError.mockRestore();
    });
  });

  describe('listenTask', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should setup process message listener', async () => {
      await worker.listenTask();
      
      expect(process.on).toHaveBeenCalledWith('message', expect.any(Function));
    });

    it('should handle worker busy scenario', async () => {
      await worker.listenTask();
      
      // Get the message handler
      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      // Set worker as busy
      worker.isBusy = true;

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData'],
        data: { test: 'data' }
      };

      messageHandler(testMessage);

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        ...testMessage,
        status: 'failed',
        reason: 'SERVER_BUSY'
      });
    });

    it('should process valid message for DatabaseInteractionWorker', async () => {
      await worker.listenTask();
      
      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const createNewDataSpy = jest.spyOn(worker, 'createNewData').mockResolvedValue(null);

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project123'],
        data: [{ tweet: 'data' }]
      };

      messageHandler(testMessage);

      expect(createNewDataSpy).toHaveBeenCalledWith({
        id: 'project123',
        data: [{ tweet: 'data' }]
      });
    });

    it('should set worker as busy during processing', async () => {
      await worker.listenTask();
      
      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      jest.spyOn(worker, 'createNewData').mockImplementation(async () => {
        expect(worker.isBusy).toBe(true);
        return null;
      });

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project123'],
        data: [{ tweet: 'data' }]
      };

      messageHandler(testMessage);
      
      // Worker should be set to not busy after processing
      await new Promise(setImmediate);
      expect(worker.isBusy).toBe(false);
    });
  });

  describe('createNewData', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should insert data successfully', async () => {
      const testData = [
        { full_text: 'Test tweet 1', created_at: '2023-01-01' },
        { full_text: 'Test tweet 2', created_at: '2023-01-02' }
      ];

      const mockInsertResult = {
        insertedCount: 2,
        insertedIds: { 0: new ObjectId(), 1: new ObjectId() },
        acknowledged: true
      };

      mockCollection.insertMany.mockResolvedValue(mockInsertResult);

      const result = await worker.createNewData({ data: testData, id: 'project123' });

      expect(mockCollection.insertMany).toHaveBeenCalledWith(testData, {
        ordered: false,
        writeConcern: {
          w: 'majority',
          journal: true
        }
      });
      expect(result).toBeNull();
    });

    it('should handle empty data array', async () => {
      const result = await worker.createNewData({ data: [], id: 'project123' });
      
      expect(mockCollection.insertMany).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle null data', async () => {
      const result = await worker.createNewData({ data: null, id: 'project123' });
      
      expect(mockCollection.insertMany).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle MongoDB insertion error', async () => {
      const testData = [{ full_text: 'Test tweet', created_at: '2023-01-01' }];
      const insertError = new Error('Database insertion failed');
      
      mockCollection.insertMany.mockRejectedValue(insertError);

      const result = await worker.createNewData({ data: testData, id: 'project123' });

      expect(result).toBeUndefined();
    });

    it('should log successful insertion', async () => {
      const testData = [{ full_text: 'Test tweet', created_at: '2023-01-01' }];
      const mockInsertResult = {
        insertedCount: 1,
        insertedIds: { 0: new ObjectId() },
        acknowledged: true
      };

      mockCollection.insertMany.mockResolvedValue(mockInsertResult);

      await worker.createNewData({ data: testData, id: 'project123' });

      expect(mockCollection.insertMany).toHaveBeenCalled();
    });
  });

  describe('getCrawledData', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should fetch data with correct aggregation pipeline', async () => {
      const queryData = {
        keyword: 'test keyword',
        start_date: '2023-01-01',
        end_date: '2023-01-31'
      };

      const mockCrawledData = [
        { _id: 'id1', full_text: 'Test tweet 1', createdAt: new Date('2023-01-15') },
        { _id: 'id2', full_text: 'Test tweet 2', createdAt: new Date('2023-01-20') }
      ];

      mockCollection.aggregate.mockReturnValue({
        toArray: jest.fn().mockResolvedValue(mockCrawledData)
      } as any);

      const result = await worker.getCrawledData({ data: queryData });

      expect(mockCollection.aggregate).toHaveBeenCalledWith([
        {
          $addFields: {
            createdAt: {
              $toDate: "$created_at"
            }
          }
        },
        {
          $match: {
            full_text: {
              $regex: 'test|keyword',
              $options: "i"
            },
            createdAt: {
              $gte: new Date('2023-01-01'),
              $lte: new Date('2023-01-31')
            }
          }
        }
      ]);

      expect(result).toEqual({
        data: mockCrawledData,
        destination: ['CrawlerWorker/onFechedData']
      });
    });

    it('should handle keyword with spaces correctly', async () => {
      const queryData = {
        keyword: 'hello world',
        start_date: '2023-01-01',
        end_date: '2023-01-31'
      };

      mockCollection.aggregate.mockReturnValue({
        toArray: jest.fn().mockResolvedValue([])
      } as any);

      await worker.getCrawledData({ data: queryData });

      expect(mockCollection.aggregate).toHaveBeenCalledWith([
        {
          $addFields: {
            createdAt: {
              $toDate: "$created_at"
            }
          }
        },
        {
          $match: {
            full_text: {
              $regex: 'hello|world',
              $options: "i"
            },
            createdAt: {
              $gte: new Date('2023-01-01'),
              $lte: new Date('2023-01-31')
            }
          }
        }
      ]);
    });

    it('should handle empty data', async () => {
      const result = await worker.getCrawledData({ data: [] });
      
      expect(mockCollection.aggregate).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle null data', async () => {
      const result = await worker.getCrawledData({ data: null });
      
      expect(mockCollection.aggregate).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle database query error', async () => {
      const queryData = {
        keyword: 'test',
        start_date: '2023-01-01',
        end_date: '2023-01-31'
      };

      const queryError = new Error('Database query failed');
      mockCollection.aggregate.mockReturnValue({
        toArray: jest.fn().mockRejectedValue(queryError)
      } as any);

      const result = await worker.getCrawledData({ data: queryData });

      expect(result).toBeUndefined();
    });

    it('should return correct data structure', async () => {
      const queryData = {
        keyword: 'test',
        start_date: '2023-01-01',
        end_date: '2023-01-31'
      };

      const mockData = [{ _id: 'id1', full_text: 'test tweet' }];
      mockCollection.aggregate.mockReturnValue({
        toArray: jest.fn().mockResolvedValue(mockData)
      } as any);

      const result = await worker.getCrawledData({ data: queryData });

      expect(result).toEqual({
        data: mockData,
        destination: ['CrawlerWorker/onFechedData']
      });
    });
  });

  describe('healthCheck', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should throw "Method not implemented" error', () => {
      expect(() => worker.healthCheck()).toThrow('Method not implemented.');
    });
  });

  describe('getInstanceId', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should return correct instance ID format', () => {
      const instanceId = worker.getInstanceId();
      
      expect(instanceId).toMatch(/^DatabaseInteractionWorker-\d+$/);
    });

    it('should return consistent instance ID', () => {
      const firstCall = worker.getInstanceId();
      const secondCall = worker.getInstanceId();
      
      expect(firstCall).toBe(secondCall);
    });
  });

  describe('Integration Tests', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should handle complete message flow for createNewData', async () => {
      await worker.listenTask();
      
      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const testData = [{ full_text: 'Test tweet', created_at: '2023-01-01' }];
      const mockInsertResult = {
        insertedCount: 1,
        insertedIds: { 0: new ObjectId() },
        acknowledged: true
      };

      mockCollection.insertMany.mockResolvedValue(mockInsertResult);

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project123'],
        data: testData
      };

      messageHandler(testMessage);

      // Wait for async processing
      await new Promise(setImmediate);

      expect(mockCollection.insertMany).toHaveBeenCalledWith(testData, expect.any(Object));
      expect(worker.isBusy).toBe(false);
    });

    it('should handle complete message flow for getCrawledData', async () => {
      await worker.listenTask();
      
      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const queryData = {
        keyword: 'test',
        start_date: '2023-01-01',
        end_date: '2023-01-31'
      };

      const mockData = [{ _id: 'id1', full_text: 'test tweet' }];
      mockCollection.aggregate.mockReturnValue({
        toArray: jest.fn().mockResolvedValue(mockData)
      } as any);

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/getCrawledData'],
        data: queryData
      };

      messageHandler(testMessage);

      // Wait for async processing
      await new Promise(setImmediate);

      expect(mockCollection.aggregate).toHaveBeenCalled();
      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'test-1',
        status: 'completed',
        data: mockData,
        destination: ['CrawlerWorker/onFechedData']
      });
    });
  });
});