#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/DevAlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 08.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import logging, time, sys, fastavro, tarfile
from ampel.base.AmpelAlert import AmpelAlert


class DevAlertProcessor():
	""" 
	For each alert: load, filter, ingest.
	"""

	def __init__(self, alert_filter, save="alert", strip_thumbnails=False):
		"""
		Instance of a t0 alert filter: 
		must implement method apply(<instance of ampel.base.AmpelAlert>)
		save: either 
			* 'alert': references to AmpelAlert instances will be kept
			* 'objectId': only objectId strings will be kept
			* 'candid': only candid integers will be kept
			* 'objectId_candid': tuple ('candid', 'objectId') will be kept
		"""
		# Setup logger
		logging.basicConfig(
			format = '%(asctime)s %(levelname)s %(message)s',
			datefmt = "%Y-%m-%d %H:%M:%S",
			level = logging.DEBUG,
			stream = sys.stdout
		)

		self._logger = logging.getLogger()
		self._alert_filter = alert_filter
		self._accepted_alerts = []
		self._rejected_alerts = []
		self.save = save
		self.strip_thumbnails = strip_thumbnails


	def get_accepted_alerts(self):
		""" """
		return self._accepted_alerts


	def get_rejeted_alerts(self):
		""" """
		return self._rejected_alerts


	def process_tar(self, tar_file_path, tar_mode="r:gz", iter_max=5000):
		"""
		For each alert: load, filter, ingest.
		"""
		self.tar_file = tarfile.open(tar_file_path, mode=tar_mode)
		iter_count = self._run(self.tar_file, self._unpack)
		return iter_count


	def process_loaded_alerts(self, list_of_alerts, iter_max=5000):
		"""
		For each alert: load, filter, ingest.
		"""
		return self._run(list_of_alerts, lambda x: x)



	def _run(self, iterable, load, iter_max=5000):
		"""
		For each alert: load, filter, ingest.
		"""

		# Part 1: Setup logging 
		#######################

		self._accepted_alerts = []
		self._rejected_alerts = []

		run_start = time.time()
		iter_count = 0

		# Iterate over alerts
		for content in iterable:

			ampel_alert = load(content) 
			if ampel_alert is None:
				break

			# filter alert
			self._filter(ampel_alert)

			if iter_count == iter_max:
				self._logger.info("Reached max number of iterations")
				break

			iter_count += 1

		self._logger.info(
			"%i alert(s) processed (time required: %is)" % 
			(iter_count, int(time.time() - run_start))
		)

		# Return number of processed alerts
		return iter_count


	def _unpack(self, tar_info):
			
		# Reach end of archive
		if tar_info is None:
			self._logger.info("Reached end of tar files")
			self.tar_file.close()
			return 

		if not tar_info.isfile():
			return

		# deserialize extracted alert content
		alert_content = self._deserialize(
			self.tar_file.extractfile(tar_info)
		)

		# Create AmpelAlert instance
		return AmpelAlert(alert_content['objectId'], *self._shape(alert_content))


	def _filter(self, ampel_alert):
		""" """
		if self._alert_filter.apply(ampel_alert) is not None:
			self._logger.info(
				"+ Ingesting %i (objectId: %s)" % 
				(ampel_alert.pps[0]['candid'], ampel_alert.tran_id)
			)
			target_array = self._accepted_alerts
		else:
			self._logger.info(
				"- Rejecting %i (objectId: %s)" % 
				(ampel_alert.pps[0]['candid'], ampel_alert.tran_id)
			)
			target_array = self._rejected_alerts

		if self.save == "alert":
			target_array.append(ampel_alert)
		elif self.save == 'objectId':
			target_array.append(ampel_alert['objectId'])
		elif self.save == 'candid':
			target_array.append(ampel_alert['candid'])
		elif self.save == 'objectId_candid':
			target_array.append(ampel_alert['objectId'], ampel_alert['candid'])


	def _deserialize(self, f):
		""" """
		reader = fastavro.reader(f)
		return next(reader, None)


	def _shape(self, alert_content):
		""" """
		if self.strip_thumbnails and 'cutout' in alert_content:
			del alert_content['cutout']
		if 'prv_candidates' in alert_content:
			pps = [el for el in alert_content['prv_candidates'] if el.get('candid') is not None]
			pps.insert(0,  alert_content['candidate'])
			return pps, [el for el in alert_content['prv_candidates'] if el.get('candid') is None]
		else:
			return [alert_content['candidate']], None
