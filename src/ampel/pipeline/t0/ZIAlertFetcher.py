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


def list_kafka():
	
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument("--broker", type=str, default="epyc.astro.washington.edu:9092")
	opts = parser.parse_args()
	
	client = pykafka.KafkaClient(opts.broker)
	for name in client.topics:
		topic = client.topics[name]
		num = 0
		for p in topic.partitions.values():
			num += p.latest_available_offset() - p.earliest_available_offset()
		print('{}: {} messages'.format(name, num))
	# print(client.topics.keys())
