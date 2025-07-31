interface DateRange {
	start: string;
	end: string;
}
import LockManager from "./LockManager";
export default class CrawlerLock extends LockManager {
	private static readonly ONE_DAY_IN_MS = 24 * 60 * 60 * 1000;

	private isAdjacentOrOverlapping(
		range1: DateRange,
		range2: DateRange
	): boolean {
		const end1 = new Date(range1.end);
		const start2 = new Date(range2.start);

		return (
			start2 <= end1 ||
			start2.getTime() <= end1.getTime() + CrawlerLock.ONE_DAY_IN_MS
		);
	}

	private mergeRanges(range1: DateRange, range2: DateRange): DateRange {
		const end1 = new Date(range1.end);
		const end2 = new Date(range2.end);

		return {
			start: range1.start,
			end: end2 > end1 ? range2.end : range1.end,
		};
	}

	public mergeDateRanges(ranges: DateRange[]): DateRange[] {
		if (ranges.length === 0) return [];

		const sortedRanges = [...ranges].sort(
			(a, b) =>
				new Date(a.start).getTime() - new Date(b.start).getTime()
		);

		const merged: DateRange[] = [];
		let currentRange = { ...sortedRanges[0] };

		for (let i = 1; i < sortedRanges.length; i++) {
			const nextRange = sortedRanges[i];

			if (this.isAdjacentOrOverlapping(currentRange, nextRange)) {
				currentRange = this.mergeRanges(currentRange, nextRange);
			} else {
				merged.push(currentRange);
				currentRange = { ...nextRange };
			}
		}

		merged.push(currentRange);
		return merged;
	}

	public groupRangesByKeyword(
		keyword: string,
		ranges: DateRange[]
	): Record<string, DateRange[]> {
		return { [keyword]: this.mergeDateRanges(ranges) };
	}
	public async getLockByKeyword(keyword: string) {
		const keys = await this.getAllLocks(keyword);
		const ranges = keys.map((key) => {
			const [keyword, start, end] = key.split(":");
			return { start, end };
		});
		ranges.sort(
			(a, b) =>
				new Date(a.start).getTime() - new Date(b.start).getTime()
		);

		return this.groupRangesByKeyword(keyword, ranges);
	}
	async checkStartDateEndDateContainsOnKeys(
		keyword,
		start,
		end
	): Promise<any> {
		console.log(
			`[CrawlerLock] Checking if date range overlaps for ${keyword} between ${start} and ${end}`
		);
		const keys = await this.getLockByKeyword(keyword);
		console.log(keys);
		const ranges = keys[keyword] || [];
		const startDate = new Date(start);
		const endDate = new Date(end);
		const dateOverlaps = [];
		for (const range of ranges) {
			const rangeStart = new Date(range.start);
			const rangeEnd = new Date(range.end);
			if (
				(startDate >= rangeStart && startDate <= rangeEnd) ||
				(endDate >= rangeStart && endDate <= rangeEnd) ||
				(startDate <= rangeStart && endDate >= rangeEnd)
			) {
				dateOverlaps.push({
					from: range.start
						? rangeStart > startDate
							? range.start
							: start
						: start,
					to: range.end
						? rangeEnd < endDate
							? range.end
							: end
						: end,
				});
			}
		}
		console.log(
			`[CrawlerLock] Overlapping date ranges for ${keyword} between ${start} and ${end}:`,
			dateOverlaps
		);
		if (dateOverlaps.length > 0) {
			return dateOverlaps;
		}

		return false;
	}

	public splitAndRemoveOverlappingRanges(
		ranges: DateRange,
		keys: any
	): DateRange[] {
		const result = [];
		const request = {
			start: ranges.start,
			end: ranges.end,
		};
		const data = keys;
		console.log(
			`[CrawlerLock] Splitting and removing overlapping ranges for request: ${JSON.stringify(
				request
			)} with data: ${JSON.stringify(data)}`
		);
		const getPreviousDay = (date: string): string => {
			const d = new Date(date);
			d.setDate(d.getDate() - 1);
			return d.toISOString().split("T")[0];
		};

		const getNextDay = (date: string): string => {
			const d = new Date(date);
			d.setDate(d.getDate() + 1);
			return d.toISOString().split("T")[0];
		};

		// Helper function to normalize dates to YYYY-MM-DD format
		const normalizeDate = (date: string): string => {
			return new Date(date).toISOString().split("T")[0];
		};

		let currentStart = request.start;

		// Sort data by start date (normalize for proper comparison)
		const sortedData = data.sort((a, b) =>
			normalizeDate(a.from).localeCompare(normalizeDate(b.from))
		);
		console.log(
			`[CrawlerLock] Sorted data for splitting: ${JSON.stringify(
				sortedData
			)}`
		);

		for (const range of sortedData) {
			const normalizedFrom = normalizeDate(range.from);
			const normalizedTo = normalizeDate(range.to);

			// If current range overlaps with our request
			if (
				normalizedFrom <= request.end &&
				normalizedTo >= currentStart
			) {
				// Add segment before overlap (if any)
				if (currentStart < normalizedFrom) {
					result.push({
						start: currentStart,
						end: getPreviousDay(normalizedFrom),
					});
				}
				// Move start to after this range
				currentStart = getNextDay(normalizedTo);
			}
		}

		// Add final segment if needed
		if (currentStart <= request.end) {
			result.push({
				start: currentStart,
				end: request.end,
			});
		}

		return result;
	}
}

