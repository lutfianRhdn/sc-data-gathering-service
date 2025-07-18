import { Message, reciveMessage, sendMessage, sendMessagetoSupervisor } from "../utils/handleMessage";
import log from "../utils/log";
import { v4 as uuidv4 } from "uuid";
import { crawl } from "../utils/tweetharvest/crawl";
import EventEmitter from "events";

type Task = {
  messageId: string;
  data: {
    access_token: string;
    keywords: string;
    start_date: Date;
    end_date: Date;
  }
}
interface DateRange {
	start: string;
	end: string;
}

type CrawlParam = {
	access_token: string;
	keyword: string;
	main_range: DateRange;
	data: any[]; // Accumulates crawled results
	splited_range: CrawlParam[];
};

import {Worker as WorkerInterface} from './Worker'
import CrawlerLock from "../utils/CrawlLockManager";
export default class CrawlerWorker implements WorkerInterface {
	private instanceId: string;
	public isBusy: boolean = false;
	public lockManager: CrawlerLock = new CrawlerLock();
	private eventEmitter: EventEmitter = new EventEmitter();


	constructor() {
		this.instanceId = `CrawlerWorker-${uuidv4()}`;
		log(`[CrawlerWorker] instanceId: ${this.instanceId} created`, "info");
		this.run().catch((error) => {
			log(
				`[CrawlerWorker] Error in constructor: ${error.message}`,
				"error"
			);
		});
	}
	onFechedData(message:Message) {
		this.eventEmitter.emit("fetchedData", message);
	}
	healthCheck(): void {
		throw new Error("Method not implemented.");
	}


async  crawling(
	param: CrawlParam,
	index: number = 0,
	nestedIndex: number = 0
): Promise<any[]> {
	const { keyword, main_range, access_token, data } = param;
	const messageId= uuidv4();															

	sendMessagetoSupervisor({
		destination: ["DatabaseInteractionWorker/getCrawledData"],
		messageId: uuidv4(),
		status: "completed",
		data: {
			keyword,
			start_date: main_range.start,
			end_date: main_range.end,
		},
	});
	const crawledDataFromDatabase =await new Promise((resolve,reject)=>
		this.eventEmitter.on("fetchedData", (message: Message) => {
			if (message.status === "completed" && message.data && messageId == message.data.messageId) {
				resolve(message.data)
			}
		})
	) as any[]
	

	const crawledDataMapped = crawledDataFromDatabase.map((tweet) => ({
		created_at: new Date(tweet.created_at),
	}));
	crawledDataMapped.sort(
		(a, b) => a.created_at.getTime() - b.created_at.getTime()
	);

	const crawledRanges = {
		from: crawledDataMapped[0].created_at.toISOString(),
		to: crawledDataMapped[
			crawledDataMapped.length - 1
		].created_at.toISOString(),
	};

	console.log(`[CrawlerWorker] Crawling started for keywords: ${keyword}`);

	// Only check for overlaps and split ranges on the first call (nestedIndex === 0)
	if (nestedIndex === 0) {
		const overlapRanges =
			await  this.lockManager.checkStartDateEndDateContainsOnKeys(
				keyword,
				main_range.start,
				main_range.end
			);

		if (overlapRanges && overlapRanges.length > 0) {
			// Add crawledRanges from database
			overlapRanges.push(crawledRanges);

			console.log(
				`Found overlapping ranges for ${keyword} between ${main_range.start} and ${main_range.end}`
			);

			const splitRanges =  this.lockManager.splitAndRemoveOverlappingRanges(
				{ ...main_range },
				overlapRanges
			);

			// Convert split ranges to CrawlParam format
			param.splited_range = splitRanges.map((range) => ({
				access_token,
				keyword,
				main_range: range,
				data: [],
				splited_range: [],
			}));
		} else {
			// No overlaps, use the main range
			param.splited_range = [
				{
					access_token,
					keyword,
					main_range,
					data: [],
					splited_range: [],
				},
			];
		}
	}

	// Base case: no more ranges to process
	if (!param.splited_range || param.splited_range.length === 0) {
		console.log(
			`[CrawlerWorker] No valid date ranges found for keywords: ${keyword}`
		);
		return param.data;
	}

	// Base case: no more ranges at current nested index
	if (nestedIndex >= param.splited_range.length) {
		console.log(
			`[CrawlerWorker] Completed all ranges for keywords: ${keyword}`
		);
		return param.data;
	}

	// Get the current range to process
	const currentRangeParam = param.splited_range[nestedIndex];
	const currentRange = currentRangeParam.main_range;

	const lockKey = `${keyword}:${currentRange.start}:${currentRange.end}`;

	try {
		// Acquire lock for current range
		await  this.lockManager.aquireLock(lockKey);

		console.log(
			`[CrawlerWorker] Processing range ${nestedIndex + 1}/${
				param.splited_range.length
			}: ${currentRange.start} to ${currentRange.end}`
		);

		// Actual crawling logic (commented out for now)
		const crawledData = await crawl({
		  ACCESS_TOKEN: access_token,
		  DEBUG_MODE: false,
		  SEARCH_KEYWORDS: keyword,
		  TARGET_TWEET_COUNT: 500,
		  OUTPUT_FILENAME: `tweets_${keyword.replace(/\s+/g, "_")}_${currentRange.start}_${currentRange.end}.csv`,
		  DELAY_EACH_TWEET_SECONDS: 0,
		  DELAY_EVERY_100_TWEETS_SECONDS: 0,
		  SEARCH_TAB: "LATEST",
		  CSV_INSERT_MODE: "REPLACE",
		  SEARCH_FROM_DATE: currentRange.start,
		  SEARCH_TO_DATE: currentRange.end,
		});

		// // Mock crawled data
		// const crawledData = {
		// 	cleanTweets: [
		// 		{
		// 			id: `${nestedIndex}_1`,
		// 			text: `Tweet from range ${nestedIndex + 1}`,
		// 			created_at: currentRange.start,
		// 		},
		// 		{
		// 			id: `${nestedIndex}_2`,
		// 			text: `Another tweet from range ${nestedIndex + 1}`,
		// 			created_at: currentRange.end,
		// 		},
		// 	],
		// };

		// Add crawled data to the param.data array
		param.data.push(...(crawledData?.cleanTweets || []));

		console.log(
			`[CrawlerWorker] Completed range ${nestedIndex + 1}, collected ${
				crawledData?.cleanTweets?.length || 0
			} tweets. Total: ${param.data.length}`
		);

		// Release lock after processing
		await  this.lockManager.releaseLock(lockKey);

		// Recursive call for next range - pass the updated param with accumulated data
		return await this.crawling(param, index, nestedIndex + 1);
	} catch (error) {
		console.error(
			`[CrawlerWorker] Error in crawling method for range ${
				nestedIndex + 1
			}: ${error.message}`
		);

		// Release lock in case of error
		try {
			await  this.lockManager.releaseLock(lockKey);
		} catch (unlockError) {
			console.error(
				`[CrawlerWorker] Error releasing lock: ${unlockError.message}`
			);
		}

		// Continue with next range even if current one fails
		return await this.crawling(param, index, nestedIndex + 1);
	}
}

	async listenTask(): Promise<void> {
		try {
			process.on("message", async (message: Message) => {
				if (this.isBusy) {
					sendMessagetoSupervisor({
						...message,
						status: "failed",
						reason: "SERVER_BUSY",
					});
				
					return;
				}
        const { data } = message;
        this.isBusy = true;
				const crawled = await this.crawling(
					{
						access_token: data.access_token,
						keyword: data.keywords,
						main_range: {
							start: data.start_date,
							end: data.end_date,
						},
						data: [],
						splited_range: [],
					}
				);
				if (crawled.length === 0) {
					log(
						`No tweets found for keywords: ${data.keywords}`,
						"warn"
					);
					sendMessagetoSupervisor({
					  messageId: message.messageId as string,
					  status: "completed",
					  reason: "No tweets found",
					  data: [],
					});
					return;
				}
				sendMessagetoSupervisor({
					...message,
					status: "completed",
					destination: [`DatabaseInteractionWorker/createNewData/${message.data.projectId}`],
					data: crawled,
				});
				this.isBusy = false;
			});
		} catch (error) {
			log(
				`[CrawlerWorker] Error in listenTask: ${error.message}`,
				"error"
			);
			sendMessagetoSupervisor({
				messageId: "error",
				status: "error",
				reason: error.message,
				data: [],
			});
		} finally {
		}
	}

	async run(): Promise<void> {
		log(
			`[CrawlerWorker] instanceId: ${this.instanceId} is running`,
			"success"
		);
		// this.listenTask().catch((error) => {
		// 	log(
		// 		`[CrawlerWorker] Error in run method: ${error.message}`,
		// 		"error"
		// 	);
		// });
		// this.healthCheck();
	}
}
new CrawlerWorker();
