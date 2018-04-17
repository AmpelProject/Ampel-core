#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ZIAlertFetcher.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : 05.03.2018
# Last Modified Date: 05.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
import fastavro, io, pykafka

import itertools
import logging
import uuid
from collections import defaultdict

log = logging.getLogger('ampel.pipeline.t0.ZIAlertFetcher')

from confluent_kafka import Consumer, TopicPartition, KafkaError, Message

class AllConsumingConsumer(object):
	"""Consume messages on all topics beginning with 'ztf_'."""
	def __init__(self, broker, **consumer_config):
		config = {
			"bootstrap.servers": broker,
			"default.topic.config": {"auto.offset.reset": "smallest"},
			"enable.auto.commit": False,
			"group.id" : uuid.uuid1(),
			"enable.partition.eof" : False, # don't emit messages on EOF
			"topic.metadata.refresh.interval.ms" : 1000, # fetch new metadata every second to pick up topics quickly
			# "debug": "all",
		}
		config.update(**consumer_config)
		self._consumer = Consumer(**config)
		
		self._consumer.subscribe(['^ztf_.*'])
		
		self._offsets = defaultdict(dict)
	
	def __del__(self):
		# NB: have to explicitly call close() here to prevent
		# rd_kafka_consumer_close() from segfaulting. See:
		# https://github.com/confluentinc/confluent-kafka-python/issues/358
		self._consumer.close()
		
	def __next__(self):
		return self.consume()
	
	def __iter__(self):
		return self
	
	def consume(self, timeout=None):
		"""
		Block until one message has arrived
		"""
		message = None
		if timeout is None:
			# wake up every second to catch SIGINT
			while message is None:
				message = self._consumer.poll(1)
		else:
			message = self._consumer.poll(timeout)
		if message.error():
			raise RuntimeError(message.error())
		else:
			self._offsets[message.topic()][message.partition()] = message.offset()
			return message
	
	def commit_offsets(self):
		"""
		Commit the offsets of all messages emitted by consume()
		
		NB: partition offsets are held across rebalances, so this may
		commit a smaller offset to a partition that was later assigned to
		another consumer. If that consumer fails before committing, this
		can result in messages being delivered more than once. Given the
		choice between at-least-once and at-most-once delivery, we choose
		the former. (exactly-once is hard)
		"""
		if len(self._offsets) == 0:
			return
		offsets = []
		for topic in self._offsets:
			for partition, offset in self._offsets[topic].items():
				offsets.append(TopicPartition(topic, partition, offset+1))
		log.debug('committing offsets: {}'.format(self._offsets))
		self._consumer.commit(offsets=offsets)
		self._offsets.clear()

class ZIAlertFetcher:
	"""
	ZI: shortcut for ZTF IPAC.
	"""
	def __init__(self, archive_db=None, bootstrap='epyc.astro.washington.edu:9092', group_name=uuid.uuid1(), timeout=1, confluent=False):
		"""
		:param archive_db: an instance of :py:class:`ampel.archive.ArchiveDB`
		:param zookeeper: ZooKeeper hosts to use for broker and topic discovery
		:type brokers: str
		:param group_name: Consumer group name to use for load-balancing
		:type group_name: bytes
		:param timeout: number of seconds to wait for a message
		"""
		# TODO: handle timeout
		self._consumer = AllConsumingConsumer(bootstrap, **{'group.id':group_name})
		
		self._archive = archive_db

	def alerts(self, limit=None):
		"""
		Generate alerts until timeout is reached
		"""
		for message in itertools.islice(self._consumer, limit):
			for alert in fastavro.reader(io.BytesIO(message.value())):
				ZIAlertLoader.filter_previous_candidates(alert['prv_candidates'])
				if self._archive is not None:
					self._archive.insert_alert(alert, 0, 0)
				yield alert
		self._consumer.commit_offsets()
	
	def __iter__(self):
		return self.alerts()

def archive_topic():
	
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument("--broker", type=str, default="epyc.astro.washington.edu:9092")
	parser.add_argument("topic", type=str)
	parser.add_argument("outfile", type=str)
	
	opts = parser.parse_args()
	
	import itertools
	import tarfile
	import time
	import os
	import pwd, grp
	import uuid, socket
	
	client = KafkaClient(opts.broker)
	topic = client.topics[opts.topic.encode('utf-8')]
	
	num = 0
	for p in topic.partitions.values():
		num += p.latest_available_offset() - p.earliest_available_offset()
	print('{}: {} messages'.format(opts.topic, num))
	
	timeout=10
	consumer_group = uuid.uuid1().hex.encode('utf-8')
	consumer = topic.get_simple_consumer(consumer_group=consumer_group,
	    consumer_timeout_ms=timeout*1e3,
	    auto_commit_enable=False,
	    queued_max_messages=1000,
	    # num_consumer_fetchers=1,
	    use_rdkafka=True)
	
	def trim_alert(payload):
		reader = fastavro.reader(io.BytesIO(payload))
		schema = reader.schema
		alert = next(reader)
		
		candid = alert['candid']
		# remove cutouts to save space
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
			
			candid, payload = trim_alert(message.value)

			ti = tarfile.TarInfo('{}/{}.avro'.format(opts.topic, candid))
			ti.size = len(payload)
			ti.mtime = time.time()
			ti.uid = os.geteuid()
			ti.uname = uid
			ti.gid = os.getegid()
			ti.gname = gid
			archive.addfile(ti, io.BytesIO(payload))
			num += 1
			num_bytes += len(message.value)
			if num % 1000 == 0:
				# consumer.commit_offsets()
				elapsed = time.time()-t0
				print('{} messages in {:.1f} seconds ({:.1f}/s, {:.2f} Mbps)'.format(num, elapsed, num/elapsed, num_bytes*8/2.**20/elapsed))

def list_kafka():
	
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument("--broker", type=str, default="epyc.astro.washington.edu:9092")
	opts = parser.parse_args()
	
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
