import CrawlerLock from '../utils/CrawlLockManager';
import LockManager from '../utils/LockManager';
import { createClient } from 'redis';

// Mock Redis client
jest.mock('redis', () => ({
  createClient: jest.fn()
}));

// Mock LockManager methods
jest.mock('../utils/LockManager');

describe('CrawlerLock', () => {
  let crawlerLock: CrawlerLock;
  let mockRedisClient: any;

  beforeEach(() => {
    // Create mock Redis client
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
    
    crawlerLock = new CrawlerLock();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('mergeDateRanges', () => {
    it('should return empty array for empty input', () => {
      const result = crawlerLock.mergeDateRanges([]);
      expect(result).toEqual([]);
    });

    it('should return single range when no merging needed', () => {
      const ranges = [
        { start: '2024-01-01', end: '2024-01-02' }
      ];
      const result = crawlerLock.mergeDateRanges(ranges);
      expect(result).toEqual(ranges);
    });

    it('should merge overlapping ranges', () => {
      const ranges = [
        { start: '2024-01-01', end: '2024-01-02' },
        { start: '2024-01-02', end: '2024-01-04' }
      ];
      const result = crawlerLock.mergeDateRanges(ranges);
      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-04' }
      ]);
    });

    it('should merge adjacent ranges (within one day)', () => {
      const ranges = [
        { start: '2024-01-01', end: '2024-01-02' },
        { start: '2024-01-03', end: '2024-01-05' }
      ];
      const result = crawlerLock.mergeDateRanges(ranges);
      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-05' }
      ]);
    });

    it('should not merge ranges that are more than one day apart', () => {
      const ranges = [
        { start: '2024-01-01', end: '2024-01-02' },
        { start: '2024-01-05', end: '2024-01-06' }
      ];
      const result = crawlerLock.mergeDateRanges(ranges);
      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-02' },
        { start: '2024-01-05', end: '2024-01-06' }
      ]);
    });

    it('should handle multiple overlapping ranges', () => {
      const ranges = [
        { start: '2024-01-01', end: '2024-01-03' },
        { start: '2024-01-02', end: '2024-01-05' },
        { start: '2024-01-04', end: '2024-01-07' }
      ];
      const result = crawlerLock.mergeDateRanges(ranges);
      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-07' }
      ]);
    });

    it('should sort ranges before merging', () => {
      const ranges = [
        { start: '2024-01-05', end: '2024-01-06' },
        { start: '2024-01-01', end: '2024-01-02' },
        { start: '2024-01-03', end: '2024-01-04' }
      ];
      const result = crawlerLock.mergeDateRanges(ranges);
      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-06' }
      ]);
    });
  });

  describe('groupRangesByKeyword', () => {
    it('should group ranges by keyword', () => {
      const keyword = 'test-keyword';
      const ranges = [
        { start: '2024-01-01', end: '2024-01-02' },
        { start: '2024-01-03', end: '2024-01-04' }
      ];

      const result = crawlerLock.groupRangesByKeyword(keyword, ranges);

      expect(result).toEqual({
        'test-keyword': [
          { start: '2024-01-01', end: '2024-01-04' }
        ]
      });
    });

    it('should handle empty ranges', () => {
      const keyword = 'test-keyword';
      const ranges: any[] = [];

      const result = crawlerLock.groupRangesByKeyword(keyword, ranges);

      expect(result).toEqual({
        'test-keyword': []
      });
    });
  });

  describe('getLockByKeyword', () => {
    it('should get locks by keyword and group them', async () => {
      const keyword = 'test-keyword';
      const mockKeys = [
        'test-keyword:2024-01-01:2024-01-02',
        'test-keyword:2024-01-03:2024-01-04'
      ];

      // Mock the getAllLocks method
      jest.spyOn(crawlerLock, 'getAllLocks').mockResolvedValue(mockKeys);

      const result = await crawlerLock.getLockByKeyword(keyword);

      expect(crawlerLock.getAllLocks).toHaveBeenCalledWith(keyword);
      expect(result).toEqual({
        'test-keyword': [
          { start: '2024-01-01', end: '2024-01-04' }
        ]
      });
    });

    it('should handle empty locks', async () => {
      const keyword = 'test-keyword';
      jest.spyOn(crawlerLock, 'getAllLocks').mockResolvedValue([]);

      const result = await crawlerLock.getLockByKeyword(keyword);

      expect(result).toEqual({
        'test-keyword': []
      });
    });

    it('should sort ranges by start date', async () => {
      const keyword = 'test-keyword';
      const mockKeys = [
        'test-keyword:2024-01-05:2024-01-06',
        'test-keyword:2024-01-01:2024-01-02',
        'test-keyword:2024-01-03:2024-01-04'
      ];

      jest.spyOn(crawlerLock, 'getAllLocks').mockResolvedValue(mockKeys);

      const result = await crawlerLock.getLockByKeyword(keyword);

      expect(result).toEqual({
        'test-keyword': [
          { start: '2024-01-01', end: '2024-01-06' }
        ]
      });
    });
  });

  describe('checkStartDateEndDateContainsOnKeys', () => {
    it('should return false when no overlapping ranges found', async () => {
      const keyword = 'test-keyword';
      const start = '2024-01-10';
      const end = '2024-01-12';

      jest.spyOn(crawlerLock, 'getLockByKeyword').mockResolvedValue({
        'test-keyword': [
          { start: '2024-01-01', end: '2024-01-05' }
        ]
      });

      const result = await crawlerLock.checkStartDateEndDateContainsOnKeys(keyword, start, end);

      expect(result).toBe(false);
    });

    it('should return overlapping ranges when found', async () => {
      const keyword = 'test-keyword';
      const start = '2024-01-03';
      const end = '2024-01-07';

      jest.spyOn(crawlerLock, 'getLockByKeyword').mockResolvedValue({
        'test-keyword': [
          { start: '2024-01-02', end: '2024-01-05' },
          { start: '2024-01-06', end: '2024-01-08' }
        ]
      });

      const result = await crawlerLock.checkStartDateEndDateContainsOnKeys(keyword, start, end);

      expect(result).toEqual([
        { from: '2024-01-03', to: '2024-01-05' },
        { from: '2024-01-06', to: '2024-01-07' }
      ]);
    });

    it('should handle start date overlap', async () => {
      const keyword = 'test-keyword';
      const start = '2024-01-03';
      const end = '2024-01-10';

      jest.spyOn(crawlerLock, 'getLockByKeyword').mockResolvedValue({
        'test-keyword': [
          { start: '2024-01-01', end: '2024-01-05' }
        ]
      });

      const result = await crawlerLock.checkStartDateEndDateContainsOnKeys(keyword, start, end);

      expect(result).toEqual([
        { from: '2024-01-03', to: '2024-01-05' }
      ]);
    });

    it('should handle end date overlap', async () => {
      const keyword = 'test-keyword';
      const start = '2024-01-01';
      const end = '2024-01-03';

      jest.spyOn(crawlerLock, 'getLockByKeyword').mockResolvedValue({
        'test-keyword': [
          { start: '2024-01-02', end: '2024-01-10' }
        ]
      });

      const result = await crawlerLock.checkStartDateEndDateContainsOnKeys(keyword, start, end);

      expect(result).toEqual([
        { from: '2024-01-02', to: '2024-01-03' }
      ]);
    });

    it('should handle complete overlap (request contains existing range)', async () => {
      const keyword = 'test-keyword';
      const start = '2024-01-01';
      const end = '2024-01-10';

      jest.spyOn(crawlerLock, 'getLockByKeyword').mockResolvedValue({
        'test-keyword': [
          { start: '2024-01-03', end: '2024-01-07' }
        ]
      });

      const result = await crawlerLock.checkStartDateEndDateContainsOnKeys(keyword, start, end);

      expect(result).toEqual([
        { from: '2024-01-03', to: '2024-01-07' }
      ]);
    });

    it('should handle empty ranges', async () => {
      const keyword = 'test-keyword';
      const start = '2024-01-01';
      const end = '2024-01-10';

      jest.spyOn(crawlerLock, 'getLockByKeyword').mockResolvedValue({
        'test-keyword': []
      });

      const result = await crawlerLock.checkStartDateEndDateContainsOnKeys(keyword, start, end);

      expect(result).toBe(false);
    });
  });

  describe('splitAndRemoveOverlappingRanges', () => {
    it('should return full range when no overlaps', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-10' };
      const keys: any[] = [];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-10' }
      ]);
    });

    it('should split range around single overlap', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-10' };
      const keys = [
        { from: '2024-01-04', to: '2024-01-06' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-03' },
        { start: '2024-01-07', end: '2024-01-10' }
      ]);
    });

    it('should split range around multiple overlaps', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-20' };
      const keys = [
        { from: '2024-01-05', to: '2024-01-07' },
        { from: '2024-01-12', to: '2024-01-15' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-04' },
        { start: '2024-01-08', end: '2024-01-11' },
        { start: '2024-01-16', end: '2024-01-20' }
      ]);
    });

    it('should handle overlap at the beginning', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-10' };
      const keys = [
        { from: '2024-01-01', to: '2024-01-03' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-04', end: '2024-01-10' }
      ]);
    });

    it('should handle overlap at the end', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-10' };
      const keys = [
        { from: '2024-01-08', to: '2024-01-10' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-07' }
      ]);
    });

    it('should handle complete overlap (no remaining ranges)', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-10' };
      const keys = [
        { from: '2024-01-01', to: '2024-01-10' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([]);
    });

    it('should handle overlaps that extend beyond request range', () => {
      const ranges = { start: '2024-01-05', end: '2024-01-15' };
      const keys = [
        { from: '2024-01-01', to: '2024-01-08' },
        { from: '2024-01-12', to: '2024-01-20' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-09', end: '2024-01-11' }
      ]);
    });

    it('should sort overlapping ranges before processing', () => {
      const ranges = { start: '2024-01-01', end: '2024-01-20' };
      const keys = [
        { from: '2024-01-12', to: '2024-01-15' },
        { from: '2024-01-05', to: '2024-01-07' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-01', end: '2024-01-04' },
        { start: '2024-01-08', end: '2024-01-11' },
        { start: '2024-01-16', end: '2024-01-20' }
      ]);
    });

    it('should normalize date formats', () => {
      const ranges = { start: '2024-01-01T00:00:00.000Z', end: '2024-01-10T23:59:59.999Z' };
      const keys = [
        { from: '2024-01-04T12:00:00.000Z', to: '2024-01-06T18:30:00.000Z' }
      ];

      const result = crawlerLock.splitAndRemoveOverlappingRanges(ranges, keys);

      expect(result).toEqual([
        { start: '2024-01-01T00:00:00.000Z', end: '2024-01-03' },
        { start: '2024-01-07', end: '2024-01-10T23:59:59.999Z' }
      ]);
    });
  });
});