const amqp = require("amqplib");

class RabbitMQService {
  constructor() {
    this.connection = null;
    this.channel = null;
    this.MAX_RETRIES = 3;
    this.RETRY_DELAY = 5000;
  }

  async connect() {
    try {
      this.connection = await amqp.connect("amqp://localhost");
      this.channel = await this.connection.createChannel();
      console.log("Connected to RabbitMQ");
    } catch (error) {
      console.error("Error connecting to RabbitMQ:", error);
      throw error;
    }
  }

  async publishWithRetry(queue, message, retryCount = 0) {
    try {
      await this.channel.assertQueue(queue, { durable: true });
      const success = this.channel.sendToQueue(
        queue,
        Buffer.from(JSON.stringify(message)),
        {
          persistent: true,
          headers: { "x-retry-count": retryCount },
        }
      );
      console.log(`Message published to ${queue}: ${JSON.stringify(message)}`);
      return success;
    } catch (error) {
      if (retryCount < this.MAX_RETRIES) {
        console.log(
          `Retrying publish... Attempt ${retryCount + 1} of ${this.MAX_RETRIES}`
        );
        await new Promise((resolve) => setTimeout(resolve, this.RETRY_DELAY));
        return this.publishWithRetry(queue, message, retryCount + 1);
      }
      console.error("Max retries reached for publishing:", error);
      throw error;
    }
  }

  async subscribe(queue, callback) {
    try {
      await this.channel.assertQueue(queue, { durable: true });

      await this.channel.consume(queue, async (msg) => {
        if (!msg) return;

        const retryCount = msg.properties.headers["x-retry-count"] || 0;

        try {
          const message = JSON.parse(msg.content.toString());
          await callback(message);
          this.channel.ack(msg);
          console.log(`Message processed successfully from ${queue}`);
        } catch (error) {
          if (retryCount < this.MAX_RETRIES) {
            console.log(
              `Processing failed, retrying... Attempt ${retryCount + 1} of ${
                this.MAX_RETRIES
              }`
            );
            this.channel.nack(msg, false, false);
            await this.publishWithRetry(
              queue,
              JSON.parse(msg.content.toString()),
              retryCount + 1
            );
          } else {
            console.error("Max retries reached for processing:", error);
            this.channel.ack(msg);
          }
        }
      });

      console.log(`Subscribed to ${queue}`);
    } catch (error) {
      console.error("Error subscribing to queue:", error);
      throw error;
    }
  }

  async close() {
    try {
      await this.channel.close();
      await this.connection.close();
      console.log("Closed RabbitMQ connection");
    } catch (error) {
      console.error("Error closing RabbitMQ connection:", error);
      throw error;
    }
  }
}

module.exports = RabbitMQService;
