
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

def test_consume_alert_stream(alert_stream):
    client = pykafka.KafkaClient(hosts="localhost:9092")
    
    topic = client.topics[b'alerts']
    assert len(topic.partitions) == 2
    
    # exactly one consumer per partition
    consumers = [topic.get_balanced_consumer(consumer_group=b'ampel', consumer_timeout_ms=1e3, auto_commit_enable=False) for p in topic.partitions]
    for c in consumers:
        assert len(c.partitions) == 1
        # fetch an alert, but don't commit the offset
        assert c.consume().offset == 0
        c.stop()
    del consumers
    
    # try again committing the offsets
    consumers = [topic.get_balanced_consumer(consumer_group=b'ampel', consumer_timeout_ms=1e3, auto_commit_enable=False) for p in topic.partitions]
    for c in consumers:
        assert c.consume().offset == 0
        c.commit_offsets()
        c.stop()
    del consumers
    
    # verify restart
    consumers = [topic.get_balanced_consumer(consumer_group=b'ampel', consumer_timeout_ms=1e3, auto_commit_enable=False) for p in topic.partitions]
    for c in consumers:
        assert c.consume().offset == 1
        c.commit_offsets()
        c.stop()
    del consumers
