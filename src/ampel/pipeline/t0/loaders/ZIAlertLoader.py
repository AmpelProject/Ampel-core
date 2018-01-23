#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/loaders/ZIAlertLoader.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, fastavro
import io
import pykafka
#from operator import itemgetter

class ZIAlertLoader:
	"""
		ZI: shortcut for ZTF IPAC.

		This class is responsible for:
			* Loading IPAC generated ZTF Alerts (using fastavro)
			* Possibly filtering 'prv_candidates' photopoints 

		For now, alerts are loaded from local files (simulated test alerts),
		later, they will be loaded through a Kafka consumer.

		The static method load_flat_pps_list() returns the transient id 
		and the associated photopoints as list of dictionaries
		The static method load_raw_dict_from_file returns the raw avro dict structure
	"""
	def __init__(self, brokers, topic, group_name=b"Ampel", timeout=1):
		"""
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
	
	def alerts(self):
		"""
		Generate alerts until timeout is reached
		"""
		for message in self._consumer:
			for alert in fastavro.reader(io.BytesIO(message.value)):
				yield alert
			self._consumer.commit_offsets()
	
	def __iter__(self):
		return self.alerts()

	@staticmethod
	def load_raw_dict_from_file(file_path):
		"""	
			Load avro alert using fastavro. 
			A dictionary instance (or None) is returned 
		"""	
		with open(file_path, "rb") as fo:
			reader = fastavro.reader(fo)
			zavro_dict = next(reader, None)

		return zavro_dict


	@staticmethod
	def get_flat_pps_list_from_file(file_path):
		"""	
			Loads an avro alert (with path file_path) using fastavro. 
			Returns a tupple: first element is the alert ID and second element is 
			a flat list of dictionaries (each containing photopoints information).
			The dictionary with index 0 in the list is the most recent photopoint.
		"""
		if isinstance(file_path, dict):
			zavro_dict = file_path
		else:
			with open(file_path, "rb") as fo:
				reader = fastavro.reader(fo)
				zavro_dict = next(reader, None)

		# Efficient way of creating the flat list of pps required for AmpelAlert
		zavro_dict['prv_candidates'].insert(0, zavro_dict['candidate'])

		return zavro_dict['objectId'], zavro_dict['prv_candidates']

		
	@staticmethod
	def filter_previous_candidates(prv_cd):
		""" Checks for None candids or photopoints with pdiffimfilename starting with /stage 
			delete the matching candidates from the previous_candidates list
			This function might not be needed for production	
		"""	
		for i in range(len(prv_cd) - 1, -1, -1):
			el = prv_cd[i]
			if el['candid'] is None or el['pdiffimfilename'].startswith('/stage'):
				del prv_cd[i]


#	@staticmethod
#	def load_alert_from_file(file_path, filter_pps_history=True, chrono_sort=False):
#		"""	
#		Load and parse a ZTF avro alert.
#		Returns an instance of AmpelAlert
#		Required parameter: file path of the avro alert
#		Optional parameter: 
#			filter_pps_history: if true, filter_previous_candidates() will be used
#			chrono_sort: sort photopoints chronologically based on 'jd' parameter
#		"""	
#		zavro_dict = ZIAlertLoader.load_raw_dict_from_file(file_path)
#
#		if filter_pps_history:
#			ZIAlertLoader.filter_previous_candidates(zavro_dict['prv_candidates'])
#
#		# quicker than sorted(zavro_dict['prv_candidates'], key=lambda k: k['jd'])
#		return prv_cd if chrono_sort is False else sorted(prv_cd, key=itemgetter("jd"))
