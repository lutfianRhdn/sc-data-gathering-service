export const workerConfig = {
  CrawlerWorker: {
    count: 2,
    cpu: 1,
    memory: 1028, // in MB
    config: {},
  },
  RabbitMQWorker: {
    count: 1,
    cpu: 1,
    memory: 1028, // in MB
    config:{}
  }
}