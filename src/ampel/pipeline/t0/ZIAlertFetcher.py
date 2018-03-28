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


class ZIAlertFetcher:
	"""
	ZI: shortcut for ZTF IPAC.
	"""
	def __init__(self, archive_db, brokers, topic, group_name=b"Ampel", timeout=1):
		"""
		:param archive_db: an instance of :py:class:`ampel.archive.ArchiveDB`
		:param brokers: Comma-separated list of kafka hosts to which to connect
		:type brokers: str
		:param topic: Topic of target Kafka stream
		:type topic: bytes
		:param group_name: Consumer group name to use for load-balancing
		:type group_name: bytes
		:param timeout: number of seconds to wait for a message
		"""
		client = pykafka.KafkaClient(brokers)
		topic = client.topics[topic]
		self._consumer = topic.get_balanced_consumer(consumer_group=group_name, consumer_timeout_ms=timeout*1e3)
		self._archive = archive_db
	

	def alerts(self):
		"""
		Generate alerts until timeout is reached
		"""
		for message in self._consumer:
			for alert in fastavro.reader(io.BytesIO(message.value)):
				ZIAlertLoader.filter_previous_candidates(alert['prv_candidates'])
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
	
	client = pykafka.KafkaClient(opts.broker)
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
	
	client = pykafka.KafkaClient(opts.broker)
	for name in reversed(sorted(client.topics)):
		print(name.decode())
		continue
		topic = client.topics[name]

		num = 0
		for p in topic.partitions.values():
			num += p.latest_available_offset() - p.earliest_available_offset()
		print('{}: {} messages'.format(name, num))
	# print(client.topics.keys())
