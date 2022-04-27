import { expect } from 'chai';
import { KafkaConsumer, KafkaProducer } from '@service/kafka';

describe.skip('Kafka', function () {
  const kafkaProducer = new KafkaProducer();
  const kafkaConsumer = new KafkaConsumer();

  const topic = 'test-topic';

  beforeEach('Connect consumer', async function () {
    await kafkaConsumer.connect();
  });

  afterEach('Disconnect consumer', async function () {
    await kafkaConsumer.disconnect();
  });

  it('should produce and consume', async function () {
    await kafkaConsumer.subscribeAndListenToTopics([topic]);

    const newMessage = {
      name: 'test',
      number: 371,
      date: new Date(),
    };
    await kafkaProducer.sendTopicMessage({
      topic,
      message: JSON.stringify(newMessage),
    });

    await kafkaConsumer
      .findMessagesContainingValue({
        value: {
          name: 'test',
        },
      })
      .then((messages) => {
        messages.forEach((message) => {
          expect(message.name).to.equal(newMessage.name);
        });
      });
  });

  it('should not get message', async function () {
    await kafkaConsumer.subscribeAndListenToTopics([]);
    await kafkaConsumer.findMessagesContainingValue({ value: { bing: 'bong' }, retries: 1 }).catch((error: Error) => {
      expect(error.message).to.include('No messages received');
    });
  });
});
