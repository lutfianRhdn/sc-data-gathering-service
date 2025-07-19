import log from "./log";

export type Message = {
	messageId: string;
	status: "completed" | "failed" | "error" | "healthy";
	destination?: string[];
	reason?: "SERVER_BUSY" | "NO_TWEET_FOUND" | string;
	data?: any;
};
export  function sendMessage({ messageId, status, reason, destination = ['supervisor'], data }: Message): void {
  process.stdout.write(JSON.stringify({ messageId, status, reason, data }) + "\n");
}
export function sendMessagetoSupervisor({
	messageId,
	status,
	reason,
	destination = ["supervisor"],
	data,
}: Message): void {
	process.send!({ status: status, messageId,destination, reason, data });
}
export function reciveMessage(): Promise<any> {
  return new Promise((resolve) => {
    process.on("message", (data: string) => {
      const messageParse = JSON.parse(data.toString());
      if (!messageParse.data) {
        console.error("Received message does not contain 'data' field"); 
        sendMessagetoSupervisor({
          messageId: messageParse.messageId || "unknown",
          status: "failed",
          reason: "Invalid message format",
          data: [],
        });
        return;
      }
      log(`Message ${messageParse.messageId} received`, );
      resolve(messageParse);
    });
  });
}