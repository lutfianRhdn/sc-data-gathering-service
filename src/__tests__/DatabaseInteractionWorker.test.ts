import DatabaseInteractionWorker from '../workers/DatabaseInteractionWorker';
import * as mongoDB from 'mongodb';
import { Message } from '../utils/handleMessage';

// Mock dependencies
jest.mock('mongodb');
jest.mock('../utils/log');
jest.mock('../utils/handleMessage', () => ({
  sendMessagetoSupervisor: jest.fn()
}));
jest.mock('../configs/env', () => ({
  DATABASE_URL: 'mongodb://localhost:27017',
  DATABASE_NAME: 'test-db',
  DATABASE_COLLECTION: 'test-collection'
}));

const { sendMessagetoSupervisor } = require('../utils/handleMessage');

describe('DatabaseInteractionWorker', () => {
  let worker: DatabaseInteractionWorker;
  let mockClient: any;
  let mockDb: any;
  let mockCollection: any;

  beforeEach(() => {
    // Mock MongoDB client and operations
    mockCollection = {
      insertMany: jest.fn(),
      aggregate: jest.fn().mockReturnValue({
        toArray: jest.fn()
      })
    };

    mockDb = {
      collection: jest.fn().mockReturnValue(mockCollection)
    };

    mockClient = {
      db: jest.fn().mockReturnValue(mockDb),
      connect: jest.fn().mockResolvedValue(undefined)
    };

    (mongoDB.MongoClient as jest.MockedClass<typeof mongoDB.MongoClient>).mockImplementation(
      () => mockClient as any
    );

    // Mock process.on to avoid actual process event listeners in tests
    jest.spyOn(process, 'on').mockImplementation((event: any, listener: any) => {
      if (event === 'message') {
        // Store the listener for manual triggering in tests
        (worker as any)._testMessageListener = listener;
      }
      return process;
    });

    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Constructor', () => {
    it('should create instance with unique ID', () => {
      worker = new DatabaseInteractionWorker();
      expect(worker.getInstanceId()).toMatch(/^DatabaseInteractionWorker-\d+$/);
    });

    it('should initialize MongoDB client with correct URL', () => {
      worker = new DatabaseInteractionWorker();
      expect(mongoDB.MongoClient).toHaveBeenCalledWith('mongodb://localhost:27017');
    });

    it('should initialize database and collection references', () => {
      worker = new DatabaseInteractionWorker();
      expect(mockClient.db).toHaveBeenCalledWith('test-db');
      expect(mockDb.collection).toHaveBeenCalledWith('test-collection');
    });

    it('should start running and setup message listener', () => {
      const runSpy = jest.spyOn(DatabaseInteractionWorker.prototype, 'run');
      worker = new DatabaseInteractionWorker();
      expect(runSpy).toHaveBeenCalled();
    });
  });

  describe('getInstanceId', () => {
    it('should return the instance ID', () => {
      worker = new DatabaseInteractionWorker();
      const instanceId = worker.getInstanceId();
      expect(instanceId).toMatch(/^DatabaseInteractionWorker-\d+$/);
    });
  });

  describe('healthCheck', () => {
    it('should throw not implemented error', () => {
      worker = new DatabaseInteractionWorker();
      expect(() => worker.healthCheck()).toThrow('Method not implemented.');
    });
  });

  describe('run', () => {
    it('should connect to MongoDB successfully', async () => {
      worker = new DatabaseInteractionWorker();
      await worker.run();
      expect(mockClient.connect).toHaveBeenCalled();
    });

    it('should handle connection errors gracefully', async () => {
      const connectError = new Error('Connection failed');
      mockClient.connect.mockRejectedValue(connectError);
      
      worker = new DatabaseInteractionWorker();
      await expect(worker.run()).resolves.not.toThrow();
    });

    it('should setup listenTask', async () => {
      const listenTaskSpy = jest.spyOn(DatabaseInteractionWorker.prototype, 'listenTask');
      worker = new DatabaseInteractionWorker();
      await worker.run();
      expect(listenTaskSpy).toHaveBeenCalled();
    });
  });

  describe('listenTask', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should handle message when worker is not busy', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project1'],
        data: [{ full_text: 'test tweet', user: 'testuser' }]
      };

      worker.isBusy = false;
      const createNewDataSpy = jest.spyOn(worker, 'createNewData').mockResolvedValue(null);

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(createNewDataSpy).toHaveBeenCalledWith({
        id: 'project1',
        data: [{ full_text: 'test tweet', user: 'testuser' }]
      });
    });

    it('should reject message when worker is busy', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project1'],
        data: []
      };

      worker.isBusy = true;

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        ...message,
        status: 'failed',
        reason: 'SERVER_BUSY'
      });
    });

    it('should handle multiple destinations for same worker', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: [
          'DatabaseInteractionWorker/createNewData/project1',
          'DatabaseInteractionWorker/getCrawledData/query1'
        ],
        data: { keyword: 'test' }
      };

      worker.isBusy = false;
      const createNewDataSpy = jest.spyOn(worker, 'createNewData').mockResolvedValue(null);
      const getCrawledDataSpy = jest.spyOn(worker, 'getCrawledData').mockResolvedValue(null);

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(createNewDataSpy).toHaveBeenCalled();
      expect(getCrawledDataSpy).toHaveBeenCalled();
    });

    it('should filter destinations correctly', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: [
          'DatabaseInteractionWorker/createNewData/project1',
          'OtherWorker/someMethod/param1'
        ],
        data: [{ full_text: 'test tweet' }]
      };

      worker.isBusy = false;
      const createNewDataSpy = jest.spyOn(worker, 'createNewData').mockResolvedValue(null);

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(createNewDataSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe('createNewData', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should insert data successfully', async () => {
      const testData = [
        { full_text: 'test tweet 1', user: 'user1' },
        { full_text: 'test tweet 2', user: 'user2' }
      ];

      const insertResult = {
        insertedCount: 2,
        insertedIds: { 0: 'id1', 1: 'id2' }
      };

      mockCollection.insertMany.mockResolvedValue(insertResult);

      const result = await worker.createNewData({ data: testData, id: 'project1' });

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
      const result = await worker.createNewData({ data: [], id: 'project1' });
      expect(mockCollection.insertMany).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle null data', async () => {
      const result = await worker.createNewData({ data: null, id: 'project1' });
      expect(mockCollection.insertMany).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle undefined data', async () => {
      const result = await worker.createNewData({ data: undefined, id: 'project1' });
      expect(mockCollection.insertMany).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle MongoDB insertion errors', async () => {
      const testData = [{ full_text: 'test tweet', user: 'user1' }];
      const error = new Error('MongoDB insertion failed');
      mockCollection.insertMany.mockRejectedValue(error);

      const result = await worker.createNewData({ data: testData, id: 'project1' });

      expect(result).toBeUndefined();
      expect(mockCollection.insertMany).toHaveBeenCalled();
    });

    it('should handle partial insertion success', async () => {
      const testData = [
        { full_text: 'test tweet 1', user: 'user1' },
        { full_text: 'test tweet 2', user: 'user2' },
        { full_text: 'test tweet 3', user: 'user3' }
      ];

      const insertResult = {
        insertedCount: 2,
        insertedIds: { 0: 'id1', 2: 'id3' }
      };

      mockCollection.insertMany.mockResolvedValue(insertResult);

      const result = await worker.createNewData({ data: testData, id: 'project1' });

      expect(result).toBeNull();
    });
  });

  describe('getCrawledData', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should retrieve crawled data successfully', async () => {
      const queryData = {
        keyword: 'test keyword',
        start_date: '2024-01-01',
        end_date: '2024-01-31'
      };

      const mockCrawledData = [
        { _id: 'id1', full_text: 'test keyword tweet 1', created_at: '2024-01-15' },
        { _id: 'id2', full_text: 'another test keyword tweet', created_at: '2024-01-20' }
      ];

      mockCollection.aggregate().toArray.mockResolvedValue(mockCrawledData);

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
              $gte: new Date('2024-01-01'),
              $lte: new Date('2024-01-31')
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
        keyword: 'hello world test',
        start_date: '2024-01-01',
        end_date: '2024-01-31'
      };

      mockCollection.aggregate().toArray.mockResolvedValue([]);

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
              $regex: 'hello|world test',
              $options: "i"
            },
            createdAt: {
              $gte: new Date('2024-01-01'),
              $lte: new Date('2024-01-31')
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

    it('should handle undefined data', async () => {
      const result = await worker.getCrawledData({ data: undefined });
      expect(mockCollection.aggregate).not.toHaveBeenCalled();
      expect(result).toBeUndefined();
    });

    it('should handle MongoDB query errors', async () => {
      const queryData = {
        keyword: 'test keyword',
        start_date: '2024-01-01',
        end_date: '2024-01-31'
      };

      const error = new Error('MongoDB query failed');
      mockCollection.aggregate().toArray.mockRejectedValue(error);

      const result = await worker.getCrawledData({ data: queryData });

      expect(result).toBeUndefined();
    });

    it('should return empty array when no data found', async () => {
      const queryData = {
        keyword: 'non-existent keyword',
        start_date: '2024-01-01',
        end_date: '2024-01-31'
      };

      mockCollection.aggregate().toArray.mockResolvedValue([]);

      const result = await worker.getCrawledData({ data: queryData });

      expect(result).toEqual({
        data: [],
        destination: ['CrawlerWorker/onFechedData']
      });
    });

    it('should handle different date formats', async () => {
      const queryData = {
        keyword: 'test',
        start_date: '2024-01-01T00:00:00.000Z',
        end_date: '2024-01-31T23:59:59.999Z'
      };

      mockCollection.aggregate().toArray.mockResolvedValue([]);

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
              $regex: 'test',
              $options: "i"
            },
            createdAt: {
              $gte: new Date('2024-01-01T00:00:00.000Z'),
              $lte: new Date('2024-01-31T23:59:59.999Z')
            }
          }
        }
      ]);
    });
  });

  describe('Integration Tests', () => {
    beforeEach(() => {
      worker = new DatabaseInteractionWorker();
    });

    it('should handle complete workflow from message to response', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project1'],
        data: [{ full_text: 'test tweet', user: 'testuser' }]
      };

      const insertResult = {
        insertedCount: 1,
        insertedIds: { 0: 'id1' }
      };

      mockCollection.insertMany.mockResolvedValue(insertResult);
      worker.isBusy = false;

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(mockCollection.insertMany).toHaveBeenCalled();
      expect(worker.isBusy).toBe(false);
    });

    it('should handle query workflow correctly', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['DatabaseInteractionWorker/getCrawledData'],
        data: {
          keyword: 'test',
          start_date: '2024-01-01',
          end_date: '2024-01-31'
        }
      };

      const mockData = [{ _id: 'id1', full_text: 'test tweet' }];
      mockCollection.aggregate().toArray.mockResolvedValue(mockData);
      worker.isBusy = false;

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(mockCollection.aggregate).toHaveBeenCalled();
      // expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
      //   messageId: 'test-msg-1',
      //   status: 'completed',
      //   data: mockData,
      //   destination: ['CrawlerWorker/onFechedData']
      // });
    });
  });
});