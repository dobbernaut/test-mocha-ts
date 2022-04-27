import { Kafka, Producer, logLevel } from 'kafkajs';

export class KafkaProducer {
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor(brokerUrl?: string) {
    this.kafka = new Kafka({
      clientId: 'test-poc-producer',
      brokers: [brokerUrl ? brokerUrl : 'localhost:29092'], // update to get this url from config
      logLevel: logLevel.NOTHING,
    });
    this.producer = this.kafka.producer();
  }

  async sendTopicMessage(record: { topic: string; message: string }): Promise<void> {
    const { topic, message } = record;
    try {
      await this.connect();
      await this.producer.send({
        topic: topic,
        messages: [{ value: message }],
      });
      await this.disconnect();
    } catch (error) {
      throw new Error(`Failed to send message. ${error}`);
    }
  }

  private async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  private async connect(): Promise<void> {
    await this.producer.connect();
  }
}
