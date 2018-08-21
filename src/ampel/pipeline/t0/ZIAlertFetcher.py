#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

import io
import itertools
import logging
import uuid
import time
import sys
from collections import defaultdict

log = logging.getLogger('ampel.pipeline.t0.ZIAlertFetcher')

from confluent_kafka import Consumer, TopicPartition, KafkaError, Message

class AllConsumingConsumer(object):
	"""Consume messages on all topics beginning with 'ztf_'."""
	def __init__(self, broker, timeout=None, topics=['^ztf_.*'], **consumer_config):
		config = {
			"bootstrap.servers": broker,
			"default.topic.config": {"auto.offset.reset": "smallest"},
			"enable.auto.commit": True,
			"auto.commit.interval.ms": 10000,
			"enable.auto.offset.store": False,
			"group.id" : uuid.uuid1(),
			"enable.partition.eof" : False, # don't emit messages on EOF
			"topic.metadata.refresh.interval.ms" : 1000, # fetch new metadata every second to pick up topics quickly
			# "debug": "all",
		}
		config.update(**consumer_config)
		self._consumer = Consumer(**config)
		
		self._consumer.subscribe(topics)
		if timeout is None:
			self._poll_interval = 1
			self._poll_attempts = sys.maxsize
		else:
			self._poll_interval = max((1, min((30, timeout))))
			self._poll_attempts = max((1, int(timeout / self._poll_interval)))
		self._timeout = timeout
		
		self._last_message = None
	
	def __del__(self):
		# NB: have to explicitly call close() here to prevent
		# rd_kafka_consumer_close() from segfaulting. See:
		# https://github.com/confluentinc/confluent-kafka-python/issues/358
		self._consumer.close()
		
	def __next__(self):
		message = self.consume()
		if message is None:
			raise StopIteration
		else:
			return message
	
	def __iter__(self):
		return self
	
	def consume(self):
		"""
		Block until one message has arrived, and return it.
		
		Messages returned to the caller marked for committal
		upon the _next_ call to consume().
		"""
		# mark the last emitted message for committal
		if self._last_message is not None:
			self._consumer.store_offsets(self._last_message)
		self._last_message = None

		message = None
		for _ in range(self._poll_attempts):
			# wake up occasionally to catch SIGINT
			message = self._consumer.poll(self._poll_interval)
			if message is not None:
				break
		else:
			return message

		if message.error():
			raise RuntimeError(message.error())
		else:
			self._last_message = message
			return message

class ZIAlertFetcher:
	"""
	ZI: shortcut for ZTF IPAC.
	
	:param bootstrap: host:port of Kafka server
	:param group_name: consumer group name. Fetchers with the same group name
	    will be load balanced and receive disjoint sets of messages
	:param partnership: if True, subscribe to ZTF partnership alerts. Otherwise,
	    subscribe only to the public alert stream
	:param timeout: time to wait for messages before giving up, in seconds
	"""
	def __init__(self, bootstrap='epyc.astro.washington.edu:9092', group_name=uuid.uuid1(), partnership=True, timeout=1):
		"""
		:param archive_db: an instance of :py:class:`ampel.archive.ArchiveDB`
		:param zookeeper: ZooKeeper hosts to use for broker and topic discovery
		:type brokers: str
		:param group_name: Consumer group name to use for load-balancing
		:type group_name: bytes
		:param timeout: number of seconds to wait for a message
		"""
		topics = ['^ztf_.*_programid1']
		if partnership:
			topics.append('^ztf_.*_programid2')
		self._consumer = AllConsumingConsumer(bootstrap, timeout=timeout, topics=topics, **{'group.id':group_name})

	def alerts(self, limit=None):
		"""
		Generate alerts until timeout is reached
		"""
		for message in itertools.islice(self._consumer, limit):
			yield io.BytesIO(message.value()), message.partition()
		log.info('timed out')

	def __iter__(self):
		return self.alerts()

def archive_topic():

    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("--broker", type=str, default="epyc.astro.washington.edu:9092")
    parser.add_argument("--strip-cutouts", action="store_true", default=False)
    parser.add_argument("topic", type=str)
    parser.add_argument("outfile", type=str)

    opts = parser.parse_args()

    import itertools
    import tarfile
    import time
    import os
    import pwd, grp
    import uuid, socket
    import fastavro
    import io

    consumer = AllConsumingConsumer(opts.broker, topics=[opts.topic], timeout=20, **{'group.id':uuid.uuid1()})

    def trim_alert(payload):
        reader = fastavro.reader(io.BytesIO(payload))
        schema = reader.schema
        alert = next(reader)

        candid = alert['candid']
        # remove cutouts to save space
        if opts.strip_cutouts:
            for k in list(alert.keys()):
                if k.startswith('cutout'):
                    del alert[k]
            with io.BytesIO() as out:
                fastavro.writer(out, schema, [alert])
                payload = out.getvalue()

        return candid, payload

    uid = pwd.getpwuid(os.geteuid()).pw_name
    gid = grp.getgrgid(os.getegid()).gr_name

    with tarfile.open(opts.outfile, 'w:gz') as archive:
        t0 = time.time()
        num = 0
        num_bytes = 0
        for message in consumer:
            candid, payload = trim_alert(message.value())
            ti = tarfile.TarInfo('{}/{}.avro'.format(opts.topic, candid))
            ti.size = len(payload)
            ti.mtime = time.time()
            ti.uid = os.geteuid()
            ti.uname = uid
            ti.gid = os.getegid()
            ti.gname = gid
            archive.addfile(ti, io.BytesIO(payload))
            num += 1
            num_bytes += len(message.value())
            if num % 1000 == 0:
                # consumer.commit_offsets()
                elapsed = time.time()-t0
                print('{} messages in {:.1f} seconds ({:.1f}/s, {:.2f} Mbps)'.format(num, elapsed, num/elapsed, num_bytes*8/2.**20/elapsed))



def list_kafka():
	
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument("--broker", type=str, default="epyc.astro.washington.edu:9092")
	opts = parser.parse_args()

	from pykafka import KafkaClient
	
	client = KafkaClient(opts.broker)
	for name in reversed(sorted(client.topics)):
		print(name.decode())
		continue
		topic = client.topics[name]

		num = 0
		for p in topic.partitions.values():
			num += p.latest_available_offset() - p.earliest_available_offset()
		print('{}: {} messages'.format(name, num))
	# print(client.topics.keys())
