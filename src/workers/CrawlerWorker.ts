import { Message, reciveMessage, sendMessage, sendMessagetoSupervisor } from "../utils/handleMessage";
import log from "../utils/log";
import { v4 as uuidv4 } from "uuid";
import { crawl } from "../utils/tweetharvest/crawl";
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
import {Worker as WorkerInterface} from './Worker'
import CrawlerLock from "../utils/CrawlLockManager";
export default class CrawlerWorker implements WorkerInterface {
	private instanceId: string;
	public isBusy: boolean = false;
  public lockManager: CrawlerLock = new CrawlerLock();
  private crawlingQueue: any 
  private resultTweets: any[] = [];
  
	// private prefixKey: string = "LOCK_";

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

  async crawling(
    access_token: string,
    keywords: string,
    start_date: string,
    end_date: string
  ) {

    
      console.log(
        `[CrawlerWorker] Crawling started for keywords: ${keywords}`
      );

      const overlapRanges = await this.lockManager.checkStartDateEndDateContainsOnKeys(
        keywords,
        start_date,
        end_date
      );
      let range = [{
        start: start_date,
        end: end_date,
      }]
      if (overlapRanges) {
        // remove the overlapping ranges from the range
        console.log(`Found overlapping ranges for ${keywords} between ${start_date} and ${end_date}`);
        range = this.lockManager.splitAndRemoveOverlappingRanges(
          { start: start_date, end: end_date },
          overlapRanges
        );
      }
      console.log('range',range)
      if (range.length === 0) {
        log(
          `[CrawlerWorker] No valid date ranges found for keywords: ${keywords}`,
          "warn"
        );
        return [];
      }
    try {
      
      this.crawlingQueue = { [keywords]: range }
      const queue = this.crawlingQueue[keywords].shift();

      this.lockManager.aquireLock(`${keywords}:${queue.start}:${queue.end}`);
    //   const crawledData = await crawl({
    //   ACCESS_TOKEN: access_token,
    //   DEBUG_MODE: false,
    //   SEARCH_KEYWORDS: keywords,
    //   TARGET_TWEET_COUNT: 500,
    //   OUTPUT_FILENAME: `tweets_${keywords.replace(/\s+/g, "_")}_${start_date}_${end_date}.csv`,
    //   DELAY_EACH_TWEET_SECONDS: 0,
    //   DELAY_EVERY_100_TWEETS_SECONDS: 0,
    //   SEARCH_TAB: "LATEST",
    //   CSV_INSERT_MODE: "REPLACE",
    //   SEARCH_FROM_DATE: start_date,
    //   SEARCH_TO_DATE: end_date,
			// });
			
			const crawledData = {cleanTweets: []};
			this.resultTweets.push(...crawledData?.cleanTweets);
      this.lockManager.releaseLock(`${keywords}:${queue.start}:${queue.end}`);
      
      // this.crawling(this)

    }catch (error) {
      log(
        `[CrawlerWorker] Error in crawling method: ${error.message}`,
        "error"
      );
      return [];
    }
    // this.lockManager.aquireLock(`${keywords}:${start_date}:${end_date}`);
        // Perform crawling logic here
      

    
    
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
					data.access_token,
					data.keyword,
					data.start_date,
					data.end_date
				);
				// const crawled =[]

				// if (crawled.length === 0) {
				// 	log(
				// 		`No tweets found for keywords: ${data.keywords}`,
				// 		"warn"
				// 	);
				// 	// sendMessagetoSupervisor({
				// 	//   messageId: message.messageId as string,
				// 	//   status: "completed",
				// 	//   reason: "No tweets found",
				// 	//   data: [],
				// 	// });
				// 	return;
				// }
				sendMessagetoSupervisor({
					...message,
					status: "completed",
					destination: `DatabaseInteractionWorker/createNewData/${message.data.projectId}`,
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


const data = [
	{
		access_token: "your_access_token",
		keywords: "example keyword",
		start_date: "2022-12-20",
		end_date: "2023-01-31",
	},
	// {
	// 	access_token: "your_access_token",
	// 	keywords: "example keyword",
	// 	start_date: "2023-01-01",
	// 	end_date: "2023-01-20",
	// },
	// {
	// 	access_token: "your_access_token",
	// 	keywords: "example keyword",
	// 	start_date: "2023-01-25",
	// 	end_date: "2023-01-31",
	// },
	// {
	// 	access_token: "your_access_token",
	// 	keywords: "example",
	// 	start_date: "2023-01-16",
	// 	end_date: "2023-01-31",
	// },
];


const crawler = new CrawlerWorker();

(async () => {
  
  const promises = data.map((item) =>
    crawler.crawling(
      item.access_token,
      item.keywords,
      item.start_date,
      item.end_date
    )
  );
  
const results = await Promise.allSettled(promises);

results.forEach((result, index) => {
	if (result.status === "fulfilled") {
		// console.log(`Item ${index} result:`, result.value);
	} else {
    // console.error(`Item ${index} error:`, result.reason);
	}
});
})()