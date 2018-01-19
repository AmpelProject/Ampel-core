
import pytest
import pykafka

def feed_test_messages():
    client = pykafka.KafkaClient(hosts='localhost:9092')
    with client.topics[b'test'].get_sync_producer() as producer:
        for i in range(4):
            msg = 'test message {}'.format(i)
            producer.produce(msg.encode('utf-8'))

def test_consume_test_mesages(kafka_server):
    feed_test_messages()
    client = pykafka.KafkaClient(hosts="localhost:9092")
    topic = client.topics[b'test']
    assert len(topic.partitions) == 1
    for idx, message in enumerate(topic.get_simple_consumer(consumer_timeout_ms=1e3)):
        assert message.offset == idx
    assert idx == 3
