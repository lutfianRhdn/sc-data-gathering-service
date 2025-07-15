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
import {Worker as WorkerInterface} from './Worker'
export default class CrawlerWorker implements WorkerInterface {
  private instanceId: string;
  public isBusy: boolean = false;

  constructor() {
    this.instanceId = `CrawlerWorker-${uuidv4()}`; 
    log(`[CrawlerWorker] instanceId: ${this.instanceId} created`, "info");
    this.run().catch((error) => {
      log(`[CrawlerWorker] Error in constructor: ${error.message}`, "error");
    });
  }

  async crawling(
    access_token: string,
    keywords: string,
    start_date: string,
    end_date: string
  ): Promise<any> {
    console.log(`[CrawlerWorker] Crawling started for keywords: ${keywords}`);
    const data = await crawl({
      ACCESS_TOKEN: access_token,
      DEBUG_MODE: false,
      SEARCH_KEYWORDS: keywords,
      TARGET_TWEET_COUNT: 500,
      OUTPUT_FILENAME: `tweets_${keywords.replace(/\s+/g, "_")}_${start_date}_${end_date}.csv`,
      DELAY_EACH_TWEET_SECONDS: 0,
      DELAY_EVERY_100_TWEETS_SECONDS: 0,
      SEARCH_TAB: "LATEST",
      CSV_INSERT_MODE: "REPLACE",
      SEARCH_FROM_DATE: start_date,
      SEARCH_TO_DATE: end_date,
    });
    log(`${data?.cleanTweets.length || 0} Crawling completed for keywords: ${keywords} `,"info"    );
    return data?.cleanTweets || [];
  }

  healthCheck(): void {
    setInterval(()=>
    sendMessagetoSupervisor({
      messageId: uuidv4(),
      status: "healthy",
      data: {
        instanceId: this.instanceId,
        timestamp: new Date().toISOString(),
      },
    }), 10000);
  }

  async listenTask(): Promise<void> {
    try {
      process.on('message', async(message: Message) => {
			if (this.isBusy) {
				sendMessagetoSupervisor({
					...message,
					status: "failed",
					reason: "SERVER_BUSY",
				});
				this.isBusy = false;
				return;
			}
			const { data } = message;
			const crawled = await this.crawling(
				data.access_token,
				data.keyword,
				data.start_date,
				data.end_date
        );
        // const crawled =[]

        if (crawled.length === 0) {
          log(`No tweets found for keywords: ${data.keywords}`, "warn");
          sendMessagetoSupervisor({
            messageId: message.messageId as string,
            status: "completed",
            reason: "No tweets found",
            data: [],
          });
          return
        }
			sendMessagetoSupervisor({
				...message,
				status: "completed",
				destination: `DatabaseInteractionWorker/createNewData/${message.data.projectId}`,
				data: crawled,
			});
			this.isBusy = false;

		})
    

    } catch (error) {
      log(`[CrawlerWorker] Error in listenTask: ${error.message}`, "error");
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
    log (`[CrawlerWorker] instanceId: ${this.instanceId} is running`, "success");
    this.listenTask().catch((error) => {
      log(`[CrawlerWorker] Error in run method: ${error.message}`, "error");
    });
    this.healthCheck();
  }
}

new CrawlerWorker()