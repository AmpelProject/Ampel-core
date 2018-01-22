
import pytest
import pykafka

def test_consume_mesages(kafka_server):
    client = pykafka.KafkaClient(hosts="localhost:9092")
    for idx, message in enumerate(client.topics[b'test'].get_simple_consumer(consumer_timeout_ms=1e3)):
        assert message.offset == idx
    assert idx == 3