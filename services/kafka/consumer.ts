import { Kafka, Consumer, logLevel } from 'kafkajs';
import { pause } from '@services/utils';

export class KafkaConsumer {
  private readonly kafka: Kafka;
  private consumer: Consumer;
  private groupId: string;
  readonly messages: unknown[] = [];

  constructor(settings?: { groupId?: string; brokerUrl?: string }) {
    this.kafka = new Kafka({
      clientId: 'test-poc-consumer',
      brokers: [settings?.brokerUrl ? settings.brokerUrl : 'localhost:29092'], // update to get this url from config
      logLevel: logLevel.NOTHING,
    });
    this.groupId = settings?.groupId ? settings.groupId : 'test-group';
  }

  async connect(): Promise<void> {
    this.consumer = this.kafka.consumer({ groupId: this.groupId });
    await this.consumer.connect();
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
  }

  async subscribeAndListenToTopics(topics: string[]): Promise<void> {
    try {
      await this.subscribeToTopics(topics);
      await this.startListening();
    } catch (error) {
      throw new Error(`Failed to subscribe and listen for messages. ${error}`);
    }
  }

  async subscribeToTopics(topics: string[]): Promise<void> {
    for (const topic of topics) {
      await this.consumer.subscribe({ topic });
    }
  }

  async startListening(): Promise<void> {
    this.messages.length = 0;
    try {
      await this.consumer.run({
        // eslint-disable-next-line require-await
        eachMessage: async ({ message }) => {
          this.messages.push(JSON.parse(message.value.toString())); // are we always expecting json messages?
        },
      });
    } catch (error) {
      throw new Error(`Failed to listen for messages. ${error}`);
    }
  }

  async findMessagesContainingValue({ value, retries = 3 }: { value: any; retries?: number }): Promise<any[]> {
    let filteredMessages: any[];
    for (let retry = 1; retry <= retries; retry++) {
      try {
        if (this.messages.length === 0) {
          throw new Error('No messages received.');
        }
        filteredMessages = this.filterMessages(value);
        if (filteredMessages.length === 0) {
          throw new Error('No messages found matching the given value.');
        }
        return filteredMessages;
      } catch (error) {
        if (retry >= retries) {
          throw new Error(error);
        }
        await pause(2);
      }
    }
  }

  private filterMessages(filter: any): unknown[] {
    return this.messages.filter((message) => {
      return Object.keys(filter).every((key) => message[key] === filter[key]);
    });
  }
}
