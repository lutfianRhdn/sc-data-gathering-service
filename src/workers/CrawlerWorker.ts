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
	static isBusy: boolean = false;
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
	onFechedData(message: Message) {
		this.eventEmitter.emit("fetchedData", message);
	}
	healthCheck(): void {
		setInterval(
			() =>
				sendMessagetoSupervisor({
					messageId: uuidv4(),
					status: "healthy",
					data: {
						instanceId: this.instanceId,
						timestamp: new Date().toISOString(),
					},
				}),
			10000
		);
	}

	async crawling(
		message:Message
	) {
		try {
			

			const { data } = message;
			log(
				`[CrawlerWorker] Received message: ${message.messageId} with keywords: ${data.keywords}, start date: ${data.start_date}, end date: ${data.end_date}`,
				"info"
			);
			const crawled = await this.getTweets({
				access_token: data.tweetToken,
				keyword: data.keyword,
				main_range: {
					start: data.start_date_crawl,
					end: data.end_date_crawl,
				},
				data: [],
				splited_range: [],
			});
			const regexFilter = new RegExp(data.keyword.replace(' ', '|'), 'i');

			const filteredCrawled = crawled.filter((c: any) =>
				regexFilter.test(c.full_text)
			);
			log(`[CrawlerWorker] Filtered crawled tweets for keyword "${data.keyword}": ${filteredCrawled.length} tweets found`, "info");
			if (crawled.length === 0) {
				log(
					`No tweets found for keywords: ${data.keyword}`,
					"warn"
				);
				// 	sendMessagetoSupervisor({
				// 		messageId: message.messageId as string,
				// 		status: "completed",
				// 		reason: "No tweets found",
				// 		data: [],
				// 	});
				// 	return;
			}
			if (filteredCrawled.length !== 0) {
				sendMessagetoSupervisor({
					...message,
					status: "completed",
					destination: [
						`DatabaseInteractionWorker/createNewData/${message.data.projectId}`,
					],
					data: filteredCrawled,
				});
			}
			const messageId = uuidv4();
			const { keyword, main_range } = data;
			setTimeout(() => {
			
				sendMessagetoSupervisor({
					destination: ["DatabaseInteractionWorker/getCrawledData"],
					messageId: messageId,
					status: "completed",
					data: {
						keyword,
						start_date: data.start_date_crawl,
						end_date: data.end_date_crawl,
					},
				});
			}, 5000)
			this.eventEmitter.on("fetchedData", (message: Message) => {
				if (
					message.status === "completed" &&
					message.messageId === messageId
				) {
					const crawledDataFromDatabase = message.data as any[];
					log(`[CrawlerWorker] Fetched ${crawledDataFromDatabase.length} tweets from database for keyword "${data.keyword}"`, "info");
					const tweetIds = crawledDataFromDatabase.map(
						(tweet) => tweet._id
					);
					sendMessagetoSupervisor({
						messageId: message.messageId,
						status: "completed",
						data: {
							projectId: data.projectId,
							keyword: data.keyword,
							start_date: data.start_date_crawl,
							end_date: data.end_date_crawl,
							// tweetIds: tweetIds,
						},
						destination: [`RabbitMQWorker/produceData/${data.projectId}`],
					});
					CrawlerWorker.isBusy = false;
					log(
						`[CrawlerWorker] Successfully processed crawling for keyword "${data.keyword}" with ${crawledDataFromDatabase.length} tweets`,
						"success")
				}
			});
		} catch (error) {
			log(
				`[CrawlerWorker] Error in crawling method: ${error.message}`,
				"error"
			);
		} finally {
			CrawlerWorker.isBusy = false;
		}
	}
	async getTweets(
		param: CrawlParam,
		index: number = 0,
		nestedIndex: number = 0
	): Promise<any[]> {
		const { keyword, main_range, access_token, data } = param;
		const messageId = uuidv4();
		sendMessagetoSupervisor({
			destination: ["DatabaseInteractionWorker/getCrawledData"],
			messageId: messageId,
			status: "completed",
			data: {
				keyword,
				start_date: main_range.start,
				end_date: main_range.end,
			},
		});
		const crawledDataFromDatabase = (await new Promise(
			(resolve, reject) =>
				this.eventEmitter.on("fetchedData", (message: Message) => {
					log(`[CrawlerWorker] Received message for keyword "${keyword}" with messageId: ${message.messageId}`, "info");
					if (
						message.status === "completed" &&
						messageId == message.messageId
					) {
						log(`[CrawlerWorker] Fetched ${message.data.length} tweets from database for keyword "${keyword}"`, "info");
						resolve(message.data);
					}
				})
		)) as any[];

		const crawledDataMapped = crawledDataFromDatabase.map((tweet) => ({
			created_at: new Date(tweet.createdAt),
		}));
		crawledDataMapped.sort(
			(a, b) => a.created_at.getTime() - b.created_at.getTime()
		);
		const crawledRanges = crawledDataMapped.length === 0 ? null : {
			from: new Date(crawledDataMapped[0]?.created_at).toISOString().split('T')[0],
			to: new Date(crawledDataMapped[
				crawledDataMapped.length - 1
			]?.created_at).toISOString().split('T')[0],
		};
		if (crawledRanges&&(crawledRanges.from === param.main_range.start && crawledRanges.to === param.main_range.end)) {
			log(`[CrawlerWorker] Data already crawled for keyword "${keyword}" between ${param.main_range.start} and ${param.main_range.end}`, "info");
			return [];
		}

		// Only check for overlaps and split ranges on the first call (nestedIndex === 0)
		if (nestedIndex === 0) {
			const overlapRanges =
				await this.lockManager.checkStartDateEndDateContainsOnKeys(
					keyword,
					main_range.start,
					main_range.end
				) || [];
			if (crawledRanges !== null && (crawledRanges.from && crawledRanges.to)) overlapRanges.push(crawledRanges);
			log(
				`[CrawlerWorker] Overlapping ranges for ${keyword} between ${main_range.start} and ${main_range.end}:`,
				"info"
			);
			if (overlapRanges && overlapRanges.length > 0) {
				log(
					`Found overlapping ranges for ${keyword} between ${main_range.start} and ${main_range.end}`,
					"info"
				);

				const splitRanges =
					this.lockManager.splitAndRemoveOverlappingRanges(
						{ ...main_range },
						overlapRanges
					);
					if (!splitRanges || splitRanges.length === 0) {
						log(
							`[CrawlerWorker] No valid split ranges found for ${keyword} between ${main_range.start} and ${main_range.end}`,
							"warn"
						);
						await new Promise((resolve) => setTimeout(resolve, 5000));
						return this.getTweets(param, index, nestedIndex );
					}

					log('[CrawlerWorker] Split ranges created successfully', "info");
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
			log(
				`[CrawlerWorker] No valid date ranges found for keywords: ${keyword}`,
				"info"
			);
			return param.data;
		}

		// Base case: no more ranges at current nested index
		if (nestedIndex >= param.splited_range.length) {
			log(
				`[CrawlerWorker] Completed all ranges for keywords: ${keyword}`,
				"info"
			);
			return param.data;
		}

		// Get the current range to process
		const currentRangeParam = param.splited_range[nestedIndex];
		const currentRange = currentRangeParam.main_range;

		const lockKey = `${keyword}:${currentRange.start}:${currentRange.end}`;

		try {
			// Acquire lock for current range
			await this.lockManager.aquireLock(lockKey);

			log(
				`[CrawlerWorker] Processing range ${nestedIndex + 1}/${
					param.splited_range.length
				}: ${currentRange.start} to ${currentRange.end}`,
				"info"
			);

			// Actual crawling logic (commented out for now)
			const crawledData:any = await crawl({
				ACCESS_TOKEN: access_token,
				DEBUG_MODE: false,
				SEARCH_KEYWORDS: keyword,
				TARGET_TWEET_COUNT: 500,
				OUTPUT_FILENAME: `tweets2_${keyword.replace(/\s+/g, "_")}_${
					currentRange.start
				}_${currentRange.end}.csv`,
				DELAY_EACH_TWEET_SECONDS: 0,
				DELAY_EVERY_100_TWEETS_SECONDS: 0,
				SEARCH_TAB: "LATEST",
				CSV_INSERT_MODE: "REPLACE",
				SEARCH_FROM_DATE: currentRange.start,
				SEARCH_TO_DATE: currentRange.end,
			}); 
		
			// Add crawled data to the param.data array
			param.data.push(...(crawledData?.cleanTweets || []));

			// Release lock after processing
			await this.lockManager.releaseLock(lockKey);
			log(
				`[CrawlerWorker] Successfully processed range ${nestedIndex + 1}/${
					param.splited_range.length
				} for keywords: ${keyword}`,
				"success"
			);
			// Recursive call for next range - pass the updated param with accumulated data
			return param.splited_range.length > 0
				? await this.getTweets(param, index, nestedIndex + 1)
				: param.data;

		} catch (error) {
			log(
				`[CrawlerWorker] Error in crawling method for range ${
					nestedIndex + 1
				}: ${error.message}`,
				"error"
			);

			// Release lock in case of error
			try {
				await this.lockManager.releaseLock(lockKey);
			} catch (unlockError) {
				log(
					`[CrawlerWorker] Error releasing lock: ${unlockError.message}`,
					"error"
				);
			}

			// Continue with next range even if current one fails
			return await this.getTweets(param, index, nestedIndex + 1);
		}
	}

	async listenTask(): Promise<void> {
		try {
			process.on("message", async (message: Message) => {
				log(`recived menssage, with status Worker ${CrawlerWorker.isBusy}`)
				if (
					CrawlerWorker.isBusy &&
					message.destination.some((d) =>
						d.includes("CrawlerWorker/crawling")
					)
				) {
					sendMessagetoSupervisor({
						...message,
						status: "failed",
						reason: "SERVER_BUSY",
					});

					return;
				}
				const { destination, data, messageId } = message;
				const dest = destination.filter((d) =>
					d.includes("CrawlerWorker")
				);
				dest.forEach(async (d) => {
					log(
						`[CrawlerWorker] Received message for destination: ${d}`,
						"info"
					);
					const destinationSplited = d.split("/");
					const path = destinationSplited[1];
					const subPath = destinationSplited[2];
					
					// Only set busy for crawling tasks
					if (path === 'crawling') {
						CrawlerWorker.isBusy = true;
					}
					
					await this[path](message);
				});

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
		this.listenTask().catch((error) => {
			log(
				`[CrawlerWorker] Error in run method: ${error.message}`,
				"error"
			);
		});
		this.healthCheck();
	}
}
// Export the class for testing, but don't auto-instantiate during testing
if (process.env.NODE_ENV !== 'test') {
  new CrawlerWorker();
}
