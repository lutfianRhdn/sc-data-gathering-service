import { v4 as uuidv4 } from "uuid";
import * as amqp from "amqplib";
import { RABBITMQ_URL } from "../configs/env";
import log from "../utils/log";

interface ScrapeConfig {
  keyword: string;
  startDate: string;
  endDate: string;
  tweetToken: string;
  projectId: string;
  language?: string;
}

class RuuTniScraper {
  private connection: amqp.Connection | null = null;
  private channel: amqp.Channel | null = null;
  private readonly queueName = "projectQueue";

  constructor() {
    this.initialize();
  }

  private async initialize(): Promise<void> {
    try {
      // Connect to RabbitMQ
      const rabbitMqUrl = RABBITMQ_URL || "amqp://admin:admin123@70.153.61.68:5672/dev";
      this.connection = await amqp.connect(rabbitMqUrl);
      this.channel = await this.connection.createChannel();
      
      // Assert queue exists
      await this.channel.assertQueue(this.queueName, { durable: true });
      
      log("[RuuTniScraper] Connected to RabbitMQ successfully", "success");
    } catch (error) {
      log(`[RuuTniScraper] Failed to connect to RabbitMQ: ${error.message}`, "error");
      throw error;
    }
  }

  public async scrapeRuuTni(config?: Partial<ScrapeConfig>): Promise<void> {
    try {
      const defaultConfig: ScrapeConfig = {
        keyword: "ruu tni",
        startDate: "2024-01-01",
        endDate: "2025-02-01",
        tweetToken: uuidv4().substring(0, 8),
        projectId: `ruu-tni-${Date.now()}`,
        language: "id" // Indonesian language filter
      };

      const scrapeConfig = { ...defaultConfig, ...config };

      // Create message for the scraping system
      const message = {
        projectId: scrapeConfig.projectId,
        keyword: scrapeConfig.keyword,
        start_date_crawl: scrapeConfig.startDate,
        end_date_crawl: scrapeConfig.endDate,
        tweetToken: scrapeConfig.tweetToken,
        // Add language filter to keyword if specified
        keywords: scrapeConfig.language ? 
          `${scrapeConfig.keyword} lang:${scrapeConfig.language}` : 
          scrapeConfig.keyword
      };

      log(`[RuuTniScraper] Starting scrape for keyword: "${scrapeConfig.keyword}"`, "info");
      log(`[RuuTniScraper] Date range: ${scrapeConfig.startDate} to ${scrapeConfig.endDate}`, "info");
      log(`[RuuTniScraper] Project ID: ${scrapeConfig.projectId}`, "info");

      // Send message to queue
      if (!this.channel) {
        throw new Error("Channel is not initialized");
      }

      const messageBuffer = Buffer.from(JSON.stringify(message));
      this.channel.sendToQueue(this.queueName, messageBuffer, { persistent: true });

      log(`[RuuTniScraper] Message sent to queue successfully`, "success");
      log(`[RuuTniScraper] Expected output file: ${scrapeConfig.tweetToken}_${scrapeConfig.keyword.replace(/\s+/g, "_")}_${scrapeConfig.startDate}_${scrapeConfig.endDate}.json`, "info");

    } catch (error) {
      log(`[RuuTniScraper] Error during scraping: ${error.message}`, "error");
      throw error;
    }
  }

  public async scrapeMultipleDateRanges(): Promise<void> {
    // Define multiple date ranges to cover different periods
    const dateRanges = [
      { startDate: "2024-01-01", endDate: "2024-03-31" },
      { startDate: "2024-04-01", endDate: "2024-06-30" },
      { startDate: "2024-07-01", endDate: "2024-09-30" },
      { startDate: "2024-10-01", endDate: "2024-12-31" },
      { startDate: "2025-01-01", endDate: "2025-02-01" }
    ];

    for (const range of dateRanges) {
      await this.scrapeRuuTni({
        startDate: range.startDate,
        endDate: range.endDate,
        projectId: `ruu-tni-${range.startDate}-${range.endDate}-${Date.now()}`
      });
      
      // Add delay between requests to avoid overwhelming the system
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  public async scrapeWithVariations(): Promise<void> {
    // Different keyword variations related to RUU TNI
    const keywordVariations = [
      "ruu tni",
      "RUU TNI",
      "ruu polri",
      "RUU Polri",
      "undang undang tni",
      "undang-undang tni",
      "revisi uu tni",
      "amandemen uu tni"
    ];

    for (const keyword of keywordVariations) {
      await this.scrapeRuuTni({
        keyword: keyword,
        projectId: `${keyword.replace(/\s+/g, "-").toLowerCase()}-${Date.now()}`
      });
      
      // Add delay between different keyword searches
      await new Promise(resolve => setTimeout(resolve, 3000));
    }
  }

  public async close(): Promise<void> {
    try {
      if (this.channel) {
        await this.channel.close();
      }
      if (this.connection) {
        await this.connection.close();
      }
      log("[RuuTniScraper] Connection closed successfully", "success");
    } catch (error) {
      log(`[RuuTniScraper] Error closing connection: ${error.message}`, "error");
    }
  }
}

// CLI usage
async function main() {
  const scraper = new RuuTniScraper();
  
  try {
    const args = process.argv.slice(2);
    const mode = args[0] || "single";

    switch (mode) {
      case "single":
        await scraper.scrapeRuuTni();
        break;
      
      case "multiple-ranges":
        await scraper.scrapeMultipleDateRanges();
        break;
      
      case "variations":
        await scraper.scrapeWithVariations();
        break;
      
      case "custom":
        const keyword = args[1] || "ruu tni";
        const startDate = args[2] || "2024-01-01";
        const endDate = args[3] || "2025-02-01";
        
        await scraper.scrapeRuuTni({
          keyword,
          startDate,
          endDate
        });
        break;
      
      default:
        console.log(`
Usage: ts-node src/scripts/scrape-ruu-tni.ts [mode] [args...]

Modes:
  single                    - Scrape with default "ruu tni" keyword
  multiple-ranges          - Scrape across multiple date ranges
  variations               - Scrape with different keyword variations
  custom [keyword] [start] [end] - Custom scraping with specified parameters

Examples:
  ts-node src/scripts/scrape-ruu-tni.ts single
  ts-node src/scripts/scrape-ruu-tni.ts multiple-ranges
  ts-node src/scripts/scrape-ruu-tni.ts variations
  ts-node src/scripts/scrape-ruu-tni.ts custom "ruu polri" "2024-01-01" "2024-12-31"
        `);
    }
    
    log("[RuuTniScraper] Script completed successfully", "success");
  } catch (error) {
    log(`[RuuTniScraper] Script failed: ${error.message}`, "error");
    process.exit(1);
  } finally {
    await scraper.close();
  }
}

// Run if this file is executed directly
if (require.main === module) {
  main();
}

export default RuuTniScraper;