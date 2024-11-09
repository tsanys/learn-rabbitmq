const RabbitMQService = require("./RabbitMQService");

async function test() {
  const rabbitMQ = new RabbitMQService();
  await rabbitMQ.connect();

  await rabbitMQ.subscribe("test-queue", async (message) => {
    console.log("Received message:", message);

    if (message.shouldFail) {
      throw new Error("Message processing failed");
    }

    console.log("Processing completed successfully");
  });

  await rabbitMQ.publishWithRetry("test-queue", {
    text: "Hello World",
    shouldFail: false,
  });

  await rabbitMQ.publishWithRetry("test-queue", {
    text: "This message will fail",
    shouldFail: true,
  });

  setTimeout(async () => {
    await rabbitMQ.close();
  }, 5000);
}

test().catch(console.error);
