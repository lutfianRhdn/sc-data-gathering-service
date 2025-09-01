import CrawlerWorker from '../workers/CrawlerWorker';
import { Message } from '../utils/handleMessage';
import EventEmitter from 'events';
import { v4 as uuidv4 } from 'uuid';

// Mock external dependencies
jest.mock('../utils/handleMessage', () => ({
  sendMessagetoSupervisor: jest.fn(),
  Message: jest.fn()
}));
jest.mock('../utils/log');
jest.mock('uuid', () => ({
  v4: jest.fn(() => 'mocked-uuid-123')
}));
jest.mock('../utils/tweetharvest/crawl', () => ({
  crawl: jest.fn()
}));
jest.mock('../utils/CrawlLockManager');

const mockSendMessagetoSupervisor = require('../utils/handleMessage').sendMessagetoSupervisor;
const mockCrawl = require('../utils/tweetharvest/crawl').crawl;
const MockCrawlerLock = require('../utils/CrawlLockManager').default;

describe('CrawlerWorker', () => {
  let worker: CrawlerWorker;
  let mockLockManager: any;

  beforeEach(() => {
    jest.clearAllMocks();
    
    // Setup CrawlerLock mock
    mockLockManager = {
      checkStartDateEndDateContainsOnKeys: jest.fn(),
      splitAndRemoveOverlappingRanges: jest.fn(),
      aquireLock: jest.fn(),
      releaseLock: jest.fn()
    };
    MockCrawlerLock.mockImplementation(() => mockLockManager);

    // Mock process.on to prevent actual event listener registration
    jest.spyOn(process, 'on').mockImplementation(() => process);
    
    // Mock setInterval for healthCheck
    jest.spyOn(global, 'setInterval').mockImplementation((callback: Function) => {
      // Don't actually set interval, just return a mock timer
      return 'mock-timer' as any;
    });

    // Reset static property
    CrawlerWorker.isBusy = false;
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  describe('Constructor', () => {
    it('should create instance with correct instanceId format', () => {
      worker = new CrawlerWorker();
      
      expect(worker['instanceId']).toBe('CrawlerWorker-mocked-uuid-123');
      expect(CrawlerWorker.isBusy).toBe(false);
    });

    it('should initialize lockManager', () => {
      worker = new CrawlerWorker();
      
      expect(MockCrawlerLock).toHaveBeenCalled();
      expect(worker.lockManager).toBeDefined();
    });

    it('should setup event emitter', () => {
      worker = new CrawlerWorker();
      
      expect(worker['eventEmitter']).toBeInstanceOf(EventEmitter);
    });

    it('should handle constructor errors gracefully', () => {
      const mockConsoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
      
      // Mock run method to throw error
      const originalRun = CrawlerWorker.prototype.run;
      CrawlerWorker.prototype.run = jest.fn().mockRejectedValue(new Error('Test error'));
      
      worker = new CrawlerWorker();
      
      // Restore original method
      CrawlerWorker.prototype.run = originalRun;
      mockConsoleError.mockRestore();
    });
  });

  describe('onFechedData', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should emit fetchedData event', () => {
      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: [{ tweet: 'data' }]
      };

      const emitSpy = jest.spyOn(worker['eventEmitter'], 'emit');

      worker.onFechedData(testMessage);

      expect(emitSpy).toHaveBeenCalledWith('fetchedData', testMessage);
    });
  });

  describe('healthCheck', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
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
          instanceId: 'CrawlerWorker-mocked-uuid-123',
          timestamp: expect.any(String)
        }
      });
    });
  });

  describe('crawling', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should process crawling message successfully', async () => {
      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test keyword',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      };

      const mockCrawledData = [
        { full_text: 'test keyword tweet 1', id: '1' },
        { full_text: 'test keyword tweet 2', id: '2' }
      ];

      jest.spyOn(worker, 'getTweets').mockResolvedValue(mockCrawledData);

      await worker.crawling(testMessage);

      expect(worker.getTweets).toHaveBeenCalledWith({
        access_token: 'token123',
        keyword: 'test keyword',
        main_range: {
          start: '2023-01-01',
          end: '2023-01-31'
        },
        data: [],
        splited_range: []
      });

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        ...testMessage,
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project123'],
        data: mockCrawledData
      });
    });

    it('should filter crawled data based on keyword', async () => {
      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test keyword',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      };

      const mockCrawledData = [
        { full_text: 'test keyword tweet', id: '1' },
        { full_text: 'unrelated tweet', id: '2' },
        { full_text: 'another test tweet', id: '3' }
      ];

      jest.spyOn(worker, 'getTweets').mockResolvedValue(mockCrawledData);

      await worker.crawling(testMessage);

      // Should only send filtered tweets that match the keyword
      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        ...testMessage,
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project123'],
        data: [
          { full_text: 'test keyword tweet', id: '1' },
          { full_text: 'another test tweet', id: '3' }
        ]
      });
    });

    it('should handle no crawled data found', async () => {
      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test keyword',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([]);

      await worker.crawling(testMessage);

      expect(worker.getTweets).toHaveBeenCalled();
      // Should not send message to database when no data found
      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        destination: ['DatabaseInteractionWorker/getCrawledData'],
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: {
          keyword: 'test keyword',
          start_date: '2023-01-01',
          end_date: '2023-01-31'
        }
      });
    });

    it('should handle getTweets error', async () => {
      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test keyword',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockRejectedValue(new Error('Crawling failed'));

      await worker.crawling(testMessage);

      expect(CrawlerWorker.isBusy).toBe(false);
    });

    it('should set up event listener for fetchedData', async () => {
      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test keyword',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([]);
      const onSpy = jest.spyOn(worker['eventEmitter'], 'on');

      await worker.crawling(testMessage);

      expect(onSpy).toHaveBeenCalledWith('fetchedData', expect.any(Function));
    });
  });

  describe('getTweets', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should get tweets without overlapping ranges', async () => {
      const param = {
        access_token: 'token123',
        keyword: 'test',
        main_range: { start: '2023-01-01', end: '2023-01-31' },
        data: [],
        splited_range: []
      };

      // Mock no existing data in database
      const mockDatabaseMessage: Message = {
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: []
      };

      // Mock no overlapping ranges
      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue([]);
      mockLockManager.aquireLock.mockResolvedValue(undefined);
      mockLockManager.releaseLock.mockResolvedValue(undefined);

      // Mock crawl function
      const mockCrawledTweets = [
        { id: '1', full_text: 'test tweet 1' },
        { id: '2', full_text: 'test tweet 2' }
      ];
      mockCrawl.mockResolvedValue({ cleanTweets: mockCrawledTweets });

      // Start the getTweets call
      const getTweetsPromise = worker.getTweets(param);

      // Simulate the fetchedData event for database response
      setTimeout(() => {
        worker['eventEmitter'].emit('fetchedData', mockDatabaseMessage);
      }, 10);

      const result = await getTweetsPromise;

      expect(result).toEqual(mockCrawledTweets);
      expect(mockCrawl).toHaveBeenCalledWith({
        ACCESS_TOKEN: 'token123',
        DEBUG_MODE: false,
        SEARCH_KEYWORDS: 'test',
        TARGET_TWEET_COUNT: 500,
        OUTPUT_FILENAME: expect.stringContaining('tweets2_test_2023-01-01_2023-01-31.csv'),
        DELAY_EACH_TWEET_SECONDS: 0,
        DELAY_EVERY_100_TWEETS_SECONDS: 0,
        SEARCH_TAB: 'LATEST',
        CSV_INSERT_MODE: 'REPLACE',
        SEARCH_FROM_DATE: '2023-01-01',
        SEARCH_TO_DATE: '2023-01-31'
      });
    });

    it('should handle overlapping ranges', async () => {
      const param = {
        access_token: 'token123',
        keyword: 'test',
        main_range: { start: '2023-01-01', end: '2023-01-31' },
        data: [],
        splited_range: []
      };

      const mockOverlapRanges = [
        { from: '2023-01-10', to: '2023-01-20' }
      ];

      const mockSplitRanges = [
        { start: '2023-01-01', end: '2023-01-09' },
        { start: '2023-01-21', end: '2023-01-31' }
      ];

      // Mock database response
      const mockDatabaseMessage: Message = {
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: []
      };

      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue(mockOverlapRanges);
      mockLockManager.splitAndRemoveOverlappingRanges.mockReturnValue(mockSplitRanges);
      mockLockManager.aquireLock.mockResolvedValue(undefined);
      mockLockManager.releaseLock.mockResolvedValue(undefined);

      const mockCrawledTweets = [{ id: '1', full_text: 'test tweet' }];
      mockCrawl.mockResolvedValue({ cleanTweets: mockCrawledTweets });

      // Start the getTweets call
      const getTweetsPromise = worker.getTweets(param);

      // Simulate the fetchedData event
      setTimeout(() => {
        worker['eventEmitter'].emit('fetchedData', mockDatabaseMessage);
      }, 10);

      const result = await getTweetsPromise;

      expect(mockLockManager.splitAndRemoveOverlappingRanges).toHaveBeenCalledWith(
        param.main_range,
        mockOverlapRanges
      );
      expect(result).toEqual([...mockCrawledTweets, ...mockCrawledTweets]); // Two split ranges
    });

    it('should handle lock acquisition and release', async () => {
      const param = {
        access_token: 'token123',
        keyword: 'test',
        main_range: { start: '2023-01-01', end: '2023-01-31' },
        data: [],
        splited_range: []
      };

      const mockDatabaseMessage: Message = {
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: []
      };

      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue([]);
      mockLockManager.aquireLock.mockResolvedValue(undefined);
      mockLockManager.releaseLock.mockResolvedValue(undefined);
      mockCrawl.mockResolvedValue({ cleanTweets: [] });

      const getTweetsPromise = worker.getTweets(param);

      setTimeout(() => {
        worker['eventEmitter'].emit('fetchedData', mockDatabaseMessage);
      }, 10);

      await getTweetsPromise;

      const expectedLockKey = 'test:2023-01-01:2023-01-31';
      expect(mockLockManager.aquireLock).toHaveBeenCalledWith(expectedLockKey);
      expect(mockLockManager.releaseLock).toHaveBeenCalledWith(expectedLockKey);
    });

    it('should handle crawl errors and release lock', async () => {
      const param = {
        access_token: 'token123',
        keyword: 'test',
        main_range: { start: '2023-01-01', end: '2023-01-31' },
        data: [],
        splited_range: []
      };

      const mockDatabaseMessage: Message = {
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: []
      };

      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue([]);
      mockLockManager.aquireLock.mockResolvedValue(undefined);
      mockLockManager.releaseLock.mockResolvedValue(undefined);
      mockCrawl.mockRejectedValue(new Error('Crawl failed'));

      const getTweetsPromise = worker.getTweets(param);

      setTimeout(() => {
        worker['eventEmitter'].emit('fetchedData', mockDatabaseMessage);
      }, 10);

      const result = await getTweetsPromise;

      const expectedLockKey = 'test:2023-01-01:2023-01-31';
      expect(mockLockManager.releaseLock).toHaveBeenCalledWith(expectedLockKey);
      expect(result).toEqual([]); // Should continue with next range even on error
    }, 10000); // Increase timeout

    it('should handle existing crawled data from database', async () => {
      const param = {
        access_token: 'token123',
        keyword: 'test',
        main_range: { start: '2023-01-01', end: '2023-01-31' },
        data: [],
        splited_range: []
      };

      // Mock existing data that covers the entire range
      const mockDatabaseMessage: Message = {
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: [
          { createdAt: '2023-01-01T00:00:00.000Z' },
          { createdAt: '2023-01-31T23:59:59.000Z' }
        ]
      };

      const getTweetsPromise = worker.getTweets(param);

      setTimeout(() => {
        worker['eventEmitter'].emit('fetchedData', mockDatabaseMessage);
      }, 10);

      const result = await getTweetsPromise;

      expect(result).toEqual([]);
      expect(mockCrawl).not.toHaveBeenCalled();
    }, 10000); // Increase timeout
  });

  describe('listenTask', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should setup process message listener', async () => {
      await worker.listenTask();

      expect(process.on).toHaveBeenCalledWith('message', expect.any(Function));
    });

    it('should handle busy worker scenario', async () => {
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      CrawlerWorker.isBusy = true;

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: { test: 'data' }
      };

      messageHandler(testMessage);

      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        ...testMessage,
        status: 'failed',
        reason: 'SERVER_BUSY'
      });
    });

    it('should process valid crawling message', async () => {
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const crawlingSpy = jest.spyOn(worker, 'crawling').mockResolvedValue(undefined);

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: { keyword: 'test' }
      };

      messageHandler(testMessage);

      expect(crawlingSpy).toHaveBeenCalledWith(testMessage);
      expect(CrawlerWorker.isBusy).toBe(true);
    });

    it('should handle non-crawling messages', async () => {
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      const onFechedDataSpy = jest.spyOn(worker, 'onFechedData').mockImplementation(() => {});

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['CrawlerWorker/onFechedData'],
        data: { test: 'data' }
      };

      messageHandler(testMessage);

      expect(onFechedDataSpy).toHaveBeenCalledWith(testMessage);
    });

    it('should handle listen task errors', async () => {
      const mockConsoleError = jest.spyOn(console, 'error').mockImplementation(() => {});

      // Force an error by making the message handler throw
      await worker.listenTask();

      const messageHandler = (process.on as jest.Mock).mock.calls.find(
        call => call[0] === 'message'
      )[1];

      jest.spyOn(worker, 'crawling').mockRejectedValue(new Error('Crawling error'));

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: { keyword: 'test' }
      };

      await expect(async () => {
        messageHandler(testMessage);
        // Wait for async error handling
        await new Promise(resolve => setTimeout(resolve, 10));
      }).not.toThrow();

      mockConsoleError.mockRestore();
    });
  });

  describe('run', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should start listening for tasks and health check', async () => {
      const listenTaskSpy = jest.spyOn(worker, 'listenTask').mockResolvedValue();
      const healthCheckSpy = jest.spyOn(worker, 'healthCheck').mockImplementation(() => {});

      await worker.run();

      expect(listenTaskSpy).toHaveBeenCalled();
      expect(healthCheckSpy).toHaveBeenCalled();
    });

    it('should handle listen task errors', async () => {
      jest.spyOn(worker, 'listenTask').mockRejectedValue(new Error('Listen error'));
      const healthCheckSpy = jest.spyOn(worker, 'healthCheck').mockImplementation(() => {});

      await worker.run();

      expect(healthCheckSpy).toHaveBeenCalled();
    });
  });

  describe('Static Properties', () => {
    it('should manage isBusy static property correctly', () => {
      expect(CrawlerWorker.isBusy).toBe(false);

      CrawlerWorker.isBusy = true;
      expect(CrawlerWorker.isBusy).toBe(true);

      CrawlerWorker.isBusy = false;
      expect(CrawlerWorker.isBusy).toBe(false);
    });
  });

  describe('Integration Tests', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should handle complete crawling workflow', async () => {
      // Setup mocks for complete workflow
      jest.spyOn(worker, 'getTweets').mockResolvedValue([
        { full_text: 'test keyword tweet', id: '1' }
      ]);

      const testMessage: Message = {
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test keyword',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      };

      await worker.crawling(testMessage);

      // Should send filtered data to DatabaseInteractionWorker
      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith({
        ...testMessage,
        status: 'completed',
        destination: ['DatabaseInteractionWorker/createNewData/project123'],
        data: [{ full_text: 'test keyword tweet', id: '1' }]
      });

      // Should also request crawled data from database (this call comes after sending data)
      expect(mockSendMessagetoSupervisor).toHaveBeenCalledWith(expect.objectContaining({
        destination: ['DatabaseInteractionWorker/getCrawledData'],
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: expect.objectContaining({
          keyword: 'test keyword',
          start_date: '2023-01-01',
          end_date: '2023-01-31'
        })
      }));
    });

    it('should handle fetchedData event properly', async () => {
      // Setup mocks
      jest.spyOn(worker, 'getTweets').mockResolvedValue([]);

      const crawlingPromise = worker.crawling({
        messageId: 'test-1',
        status: 'completed',
        data: {
          tweetToken: 'token123',
          keyword: 'test',
          start_date_crawl: '2023-01-01',
          end_date_crawl: '2023-01-31',
          projectId: 'project123'
        }
      });

      // Wait for the crawling to set up event listener
      await new Promise(resolve => setTimeout(resolve, 100));

      // Simulate fetchedData event
      const fetchedDataMessage: Message = {
        messageId: 'mocked-uuid-123',
        status: 'completed',
        data: [
          { _id: 'tweet1', full_text: 'test tweet 1' },
          { _id: 'tweet2', full_text: 'test tweet 2' }
        ]
      };

      worker.onFechedData(fetchedDataMessage);

      await crawlingPromise;

      // Should eventually send final result to RabbitMQWorker
      // The test will verify one of the calls matches this pattern
      const rabbitMQCalls = mockSendMessagetoSupervisor.mock.calls.filter(call => 
        call[0].destination && call[0].destination.some(dest => dest.includes('RabbitMQWorker'))
      );
      
      expect(rabbitMQCalls.length).toBeGreaterThan(0);
      expect(CrawlerWorker.isBusy).toBe(false);
    }, 10000); // Increase timeout
  });
});