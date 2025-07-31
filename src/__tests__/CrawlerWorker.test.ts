import CrawlerWorker from '../workers/CrawlerWorker';
import { Message } from '../utils/handleMessage';
import CrawlerLock from '../utils/CrawlLockManager';
import { crawl } from '../utils/tweetharvest/crawl';
import EventEmitter from 'events';

// Mock dependencies
jest.mock('../utils/handleMessage', () => ({
  reciveMessage: jest.fn(),
  sendMessage: jest.fn(),
  sendMessagetoSupervisor: jest.fn()
}));
jest.mock('../utils/log');
jest.mock('../utils/tweetharvest/crawl');
jest.mock('../utils/CrawlLockManager');
jest.mock('uuid', () => ({
  v4: jest.fn(() => 'test-uuid-1234')
}));

const { sendMessagetoSupervisor } = require('../utils/handleMessage');

describe('CrawlerWorker', () => {
  let worker: CrawlerWorker;
  let mockLockManager: jest.Mocked<CrawlerLock>;

  beforeEach(() => {
    // Mock CrawlerLock
    mockLockManager = {
      aquireLock: jest.fn().mockResolvedValue(undefined),
      releaseLock: jest.fn().mockResolvedValue(undefined),
      checkStartDateEndDateContainsOnKeys: jest.fn().mockResolvedValue(false),
      splitAndRemoveOverlappingRanges: jest.fn().mockReturnValue([]),
      getLockByKeyword: jest.fn().mockResolvedValue({}),
      mergeDateRanges: jest.fn().mockReturnValue([]),
      groupRangesByKeyword: jest.fn().mockReturnValue({}),
      checkLock: jest.fn().mockResolvedValue(false),
      getAllLocks: jest.fn().mockResolvedValue([]),
      relaseAllLocks: jest.fn()
    } as any;

    (CrawlerLock as jest.MockedClass<typeof CrawlerLock>).mockImplementation(() => mockLockManager);

    // Mock crawl function
    (crawl as jest.MockedFunction<typeof crawl>).mockResolvedValue({
      fileName: 'test-file.csv',
      cleanTweets: [
        { full_text: 'test tweet 1', user: 'user1', created_at: '2024-01-01' },
        { full_text: 'test tweet 2', user: 'user2', created_at: '2024-01-02' }
      ]
    });

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
      worker = new CrawlerWorker();
      expect((worker as any).instanceId).toMatch(/^CrawlerWorker-test-uuid-1234$/);
    });

    it('should initialize lock manager', () => {
      worker = new CrawlerWorker();
      expect(worker.lockManager).toBeInstanceOf(CrawlerLock);
    });

    it('should initialize event emitter', () => {
      worker = new CrawlerWorker();
      expect((worker as any).eventEmitter).toBeInstanceOf(EventEmitter);
    });

    it('should start running process', () => {
      const runSpy = jest.spyOn(CrawlerWorker.prototype, 'run');
      worker = new CrawlerWorker();
      expect(runSpy).toHaveBeenCalled();
    });

    it('should initialize isBusy as false', () => {
      worker = new CrawlerWorker();
      expect(CrawlerWorker.isBusy).toBe(false);
    });
  });

  describe('healthCheck', () => {
    it('should send health check messages periodically', () => {
      jest.useFakeTimers();
      worker = new CrawlerWorker();
      
      worker.healthCheck();
      
      // Fast-forward 10 seconds
      jest.advanceTimersByTime(10000);
      
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'test-uuid-1234',
        status: 'healthy',
        data: {
          instanceId: expect.any(String),
          timestamp: expect.any(String)
        }
      });
      
      jest.useRealTimers();
    });
  });

  describe('onFechedData', () => {
    it('should emit fetchedData event', () => {
      worker = new CrawlerWorker();
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: []
      };

      const emitSpy = jest.spyOn((worker as any).eventEmitter, 'emit');
      
      worker.onFechedData(message);
      
      expect(emitSpy).toHaveBeenCalledWith('fetchedData', message);
    });
  });

  describe('crawling', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
      CrawlerWorker.isBusy = false;
    });

    it('should process crawling message successfully', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'test keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      const getTweetsSpy = jest.spyOn(worker, 'getTweets').mockResolvedValue([
        { full_text: 'test keyword tweet', user: 'user1' },
        { full_text: 'another test keyword tweet', user: 'user2' }
      ]);

      await worker.crawling(message);

      expect(getTweetsSpy).toHaveBeenCalledWith({
        access_token: 'test-token',
        keyword: 'test keyword',
        main_range: {
          start: '2024-01-01',
          end: '2024-01-31'
        },
        data: [],
        splited_range: []
      });
    });

    it('should filter tweets by keyword', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'specific keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([
        { full_text: 'specific keyword tweet', user: 'user1' },
        { full_text: 'unrelated tweet', user: 'user2' },
        { full_text: 'another specific keyword tweet', user: 'user3' }
      ]);

      await worker.crawling(message);

      expect(sendMessagetoSupervisor).toHaveBeenCalledWith(
        expect.objectContaining({
          data: [
            { full_text: 'specific keyword tweet', user: 'user1' },
            { full_text: 'another specific keyword tweet', user: 'user3' }
          ]
        })
      );
    });

    it('should handle keywords with spaces in regex filter', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'hello world',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([
        { full_text: 'hello world tweet', user: 'user1' },
        { full_text: 'hello there tweet', user: 'user2' },
        { full_text: 'world peace tweet', user: 'user3' },
        { full_text: 'unrelated tweet', user: 'user4' }
      ]);

      await worker.crawling(message);

      // Should filter tweets containing 'hello' OR 'world'
      expect(sendMessagetoSupervisor).toHaveBeenCalledWith(
        expect.objectContaining({
          data: [
            { full_text: 'hello world tweet', user: 'user1' },
            { full_text: 'hello there tweet', user: 'user2' },
            { full_text: 'world peace tweet', user: 'user3' }
          ]
        })
      );
    });

    it('should handle empty crawled results', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'nonexistent keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([]);

      await worker.crawling(message);

      // Should not send to supervisor when no tweets found, but should continue processing
      expect(CrawlerWorker.isBusy).toBe(false);
    });

    it('should send database query after processing', async () => {
      jest.useFakeTimers();
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'test keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([
        { full_text: 'test keyword tweet', user: 'user1' }
      ]);

      await worker.crawling(message);

      // Fast-forward 5 seconds to trigger setTimeout
      jest.advanceTimersByTime(5000);

      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        destination: ['DatabaseInteractionWorker/getCrawledData'],
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: {
          keyword: 'test keyword',
          start_date: '2024-01-01',
          end_date: '2024-01-31'
        }
      });

      jest.useRealTimers();
    });

    it('should handle event emitter for fetched data', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'test keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([
        { full_text: 'test keyword tweet', user: 'user1' }
      ]);

      await worker.crawling(message);

      // Simulate event emitter response
      const fetchedDataMessage: Message = {
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: [
          { _id: 'tweet1', full_text: 'test keyword tweet' }
        ]
      };

      (worker as any).eventEmitter.emit('fetchedData', fetchedDataMessage);

      expect(sendMessagetoSupervisor).toHaveBeenCalledWith(
        expect.objectContaining({
          data: {
            projectId: 'project-123',
            keyword: 'test keyword',
            start_date: '2024-01-01',
            end_date: '2024-01-31'
          },
          destination: ['RabbitMQWorker/produceData/project-123']
        })
      );
    });

    it('should handle crawling errors gracefully', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'test keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      const crawlError = new Error('Crawling failed');
      jest.spyOn(worker, 'getTweets').mockRejectedValue(crawlError);

      await worker.crawling(message);

      expect(CrawlerWorker.isBusy).toBe(false);
    });

    it('should set and reset busy state correctly', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: {
          tweetToken: 'test-token',
          keyword: 'test keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      jest.spyOn(worker, 'getTweets').mockResolvedValue([]);

      await worker.crawling(message);

      expect(CrawlerWorker.isBusy).toBe(false);
    });
  });

  describe('getTweets', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should return empty array when no valid date ranges', async () => {
      const param = {
        access_token: 'test-token',
        keyword: 'test',
        main_range: { start: '2024-01-01', end: '2024-01-31' },
        data: [],
        splited_range: []
      };

      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue(false);

      const result = await worker.getTweets(param, 0, 0);

      expect(result).toEqual([]);
    });

    it('should handle overlapping ranges and split them', async () => {
      const param = {
        access_token: 'test-token',
        keyword: 'test',
        main_range: { start: '2024-01-01', end: '2024-01-31' },
        data: [],
        splited_range: []
      };

      const overlapRanges = [
        { from: '2024-01-10', to: '2024-01-15' }
      ];

      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue(overlapRanges);
      mockLockManager.splitAndRemoveOverlappingRanges.mockReturnValue([
        { start: '2024-01-01', end: '2024-01-09' },
        { start: '2024-01-16', end: '2024-01-31' }
      ]);

      // Mock database response
      const fetchedDataMessage: Message = {
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: []
      };

      setTimeout(() => {
        (worker as any).eventEmitter.emit('fetchedData', fetchedDataMessage);
      }, 100);

      const result = await worker.getTweets(param, 0, 0);

      expect(mockLockManager.checkStartDateEndDateContainsOnKeys).toHaveBeenCalledWith(
        'test',
        '2024-01-01',
        '2024-01-31'
      );
      expect(mockLockManager.splitAndRemoveOverlappingRanges).toHaveBeenCalled();
    });

    it('should acquire and release locks for each range', async () => {
      const param = {
        access_token: 'test-token',
        keyword: 'test',
        main_range: { start: '2024-01-01', end: '2024-01-05' },
        data: [],
        splited_range: [
          {
            access_token: 'test-token',
            keyword: 'test',
            main_range: { start: '2024-01-01', end: '2024-01-05' },
            data: [],
            splited_range: []
          }
        ]
      };

      // Mock database response
      const fetchedDataMessage: Message = {
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: []
      };

      setTimeout(() => {
        (worker as any).eventEmitter.emit('fetchedData', fetchedDataMessage);
      }, 100);

      const result = await worker.getTweets(param, 0, 0);

      expect(mockLockManager.aquireLock).toHaveBeenCalledWith('test:2024-01-01:2024-01-05');
      expect(mockLockManager.releaseLock).toHaveBeenCalledWith('test:2024-01-01:2024-01-05');
    });

    it('should handle crawl function and accumulate data', async () => {
      const param = {
        access_token: 'test-token',
        keyword: 'test',
        main_range: { start: '2024-01-01', end: '2024-01-05' },
        data: [],
        splited_range: [
          {
            access_token: 'test-token',
            keyword: 'test',
            main_range: { start: '2024-01-01', end: '2024-01-05' },
            data: [],
            splited_range: []
          }
        ]
      };

      const mockCrawlResult = {
        fileName: 'test-file.csv',
        cleanTweets: [
          { full_text: 'test tweet 1', user: 'user1' },
          { full_text: 'test tweet 2', user: 'user2' }
        ]
      };

      (crawl as jest.MockedFunction<typeof crawl>).mockResolvedValue(mockCrawlResult);

      // Mock database response
      const fetchedDataMessage: Message = {
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: []
      };

      setTimeout(() => {
        (worker as any).eventEmitter.emit('fetchedData', fetchedDataMessage);
      }, 100);

      const result = await worker.getTweets(param, 0, 0);

      expect(crawl).toHaveBeenCalledWith({
        ACCESS_TOKEN: 'test-token',
        DEBUG_MODE: false,
        SEARCH_KEYWORDS: 'test',
        TARGET_TWEET_COUNT: 500,
        OUTPUT_FILENAME: expect.stringContaining('tweets2_test_2024-01-01_2024-01-05.csv'),
        DELAY_EACH_TWEET_SECONDS: 0,
        DELAY_EVERY_100_TWEETS_SECONDS: 0,
        SEARCH_TAB: 'LATEST',
        CSV_INSERT_MODE: 'REPLACE',
        SEARCH_FROM_DATE: '2024-01-01',
        SEARCH_TO_DATE: '2024-01-05'
      });

      expect(result).toEqual(mockCrawlResult.cleanTweets);
    });

    it('should handle recursive calls for multiple ranges', async () => {
      const param = {
        access_token: 'test-token',
        keyword: 'test',
        main_range: { start: '2024-01-01', end: '2024-01-31' },
        data: [],
        splited_range: [
          {
            access_token: 'test-token',
            keyword: 'test',
            main_range: { start: '2024-01-01', end: '2024-01-15' },
            data: [],
            splited_range: []
          },
          {
            access_token: 'test-token',
            keyword: 'test',
            main_range: { start: '2024-01-16', end: '2024-01-31' },
            data: [],
            splited_range: []
          }
        ]
      };

      (crawl as jest.MockedFunction<typeof crawl>).mockResolvedValue({
        fileName: 'test-file.csv',
        cleanTweets: [{ full_text: 'test tweet', user: 'user1' }]
      });

      // Mock database response
      const fetchedDataMessage: Message = {
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: []
      };

      setTimeout(() => {
        (worker as any).eventEmitter.emit('fetchedData', fetchedDataMessage);
      }, 100);

      const result = await worker.getTweets(param, 0, 0);

      expect(crawl).toHaveBeenCalledTimes(2);
      expect(result).toEqual([
        { full_text: 'test tweet', user: 'user1' },
        { full_text: 'test tweet', user: 'user1' }
      ]);
    });


    it('should handle database response with existing crawled data', async () => {
      const param = {
			access_token: "4a71e93a225ea29ea5886aa5f6775b5a268cde4c",
			keyword: "meme jokowi",
			main_range: { start: "2024-01-01", end: "2024-01-31" },
			data: [],
			splited_range: [],
		};
      const fetchedDataMessage: Message = {
        messageId: 'test-uuid-1234',
        status: 'completed',
        data: [
          { createdAt: '2024-01-01T00:00:00.000Z' },
          { createdAt: '2024-01-31T23:59:59.999Z' }
        ]
      };

      setTimeout(() => {
        (worker as any).eventEmitter.emit('fetchedData', fetchedDataMessage);
      }, 100);

      // const result = await worker.getTweets(param, 0, 0);
      const result = worker.getTweets(param);
      expect(result).toEqual([]);
    });
  });

  describe('listenTask', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should handle incoming crawling messages when not busy', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: {
          tweetToken: 'test-token',
          keyword: 'test keyword',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      CrawlerWorker.isBusy = false;
      // const crawlingSpy = jest.spyOn(worker, 'crawling').mockResolvedValue();

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      // expect(crawlingSpy).toHaveBeenCalledWith(message);
      expect(CrawlerWorker.isBusy).toBe(true);
    });

    it('should reject messages when worker is busy', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: {}
      };

      CrawlerWorker.isBusy = true;

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

    it('should filter destinations correctly', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: [
          'CrawlerWorker/crawling',
          'OtherWorker/someMethod'
        ],
        data: {}
      };

      CrawlerWorker.isBusy = false;
      const crawlingSpy = jest.spyOn(worker, 'crawling').mockResolvedValue();

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(crawlingSpy).toHaveBeenCalledTimes(1);
    });

    it('should handle errors in message processing', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: {}
      };

      CrawlerWorker.isBusy = false;
      jest.spyOn(worker, 'crawling').mockRejectedValue(new Error('Processing failed'));

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(sendMessagetoSupervisor).toHaveBeenCalledWith({
        messageId: 'error',
        status: 'error',
        reason: 'Processing failed',
        data: []
      });
    });

    it('should handle non-crawling destinations', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['CrawlerWorker/onFechedData'],
        data: []
      };

      CrawlerWorker.isBusy = false;
      const onFechedDataSpy = jest.spyOn(worker, 'onFechedData');

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(onFechedDataSpy).toHaveBeenCalledWith(message);
    });
  });

  describe('run', () => {
    it('should initialize all components', async () => {
      const listenTaskSpy = jest.spyOn(CrawlerWorker.prototype, 'listenTask');
      const healthCheckSpy = jest.spyOn(CrawlerWorker.prototype, 'healthCheck');
      
      worker = new CrawlerWorker();
      await worker.run();
      
      expect(listenTaskSpy).toHaveBeenCalled();
      expect(healthCheckSpy).toHaveBeenCalled();
    });

    it('should handle run errors gracefully', async () => {
      const listenTaskError = new Error('Listen task failed');
      jest.spyOn(CrawlerWorker.prototype, 'listenTask').mockRejectedValue(listenTaskError);
      
      worker = new CrawlerWorker();
      await worker.run();
      
      // Should handle error gracefully without throwing
      expect(true).toBe(true);
    });
  });

  describe('Static Properties', () => {
    it('should manage isBusy state correctly', () => {
      CrawlerWorker.isBusy = false;
      expect(CrawlerWorker.isBusy).toBe(false);
      
      CrawlerWorker.isBusy = true;
      expect(CrawlerWorker.isBusy).toBe(true);
    });
  });

  describe('Integration Tests', () => {
    beforeEach(() => {
      worker = new CrawlerWorker();
    });

    it('should handle complete crawling workflow', async () => {
      const message: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        destination: ['CrawlerWorker/crawling'],
        data: {
          tweetToken: 'test-token',
          keyword: 'integration test',
          start_date_crawl: '2024-01-01',
          end_date_crawl: '2024-01-31',
          projectId: 'project-123'
        }
      };

      mockLockManager.checkStartDateEndDateContainsOnKeys.mockResolvedValue(false);
      
      (crawl as jest.MockedFunction<typeof crawl>).mockResolvedValue({
        fileName: 'test-file.csv',
        cleanTweets: [
          { full_text: 'integration test tweet', user: 'user1' }
        ]
      });

      CrawlerWorker.isBusy = false;

      await worker.listenTask();
      
      // Manually trigger the message listener
      if ((worker as any)._testMessageListener) {
        await (worker as any)._testMessageListener(message);
      }

      expect(sendMessagetoSupervisor).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'completed',
          destination: ['DatabaseInteractionWorker/createNewData/project-123'],
          data: [{ full_text: 'integration test tweet', user: 'user1' }]
        })
      );
    });

    it('should handle event emitter communication', () => {
      const testMessage: Message = {
        messageId: 'test-msg-1',
        status: 'completed',
        data: [{ _id: 'tweet1' }]
      };

      // Setup event listener
      (worker as any).eventEmitter.on('fetchedData', (message: Message) => {
        expect(message).toEqual(testMessage);
      });

      // Trigger event
      worker.onFechedData(testMessage);
    });
  });
});