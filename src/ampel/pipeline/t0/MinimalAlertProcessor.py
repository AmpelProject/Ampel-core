#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/MinimalAlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 07.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import logging, time, sys, fastavro, tarfile
from ampel.pipeline.t0.AmpelAlert import AmpelAlert


class MinimalAlertProcessor():
	""" 
	For each alert: load, filter, ingest.
	"""

	def __init__(self, alert_filter):
		"""
		Instance of a t0 alert filter: 
		must implement method apply(<instance of ampel.base.AmpelAlert>)
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

	def get_accepted_alerts(self):
		""" """
		return self._accepted_alerts


	def get_rejeted_alerts(self):
		""" """
		return self._rejected_alerts


	def run(self, tar_file_path, iter_max=5000):
		"""
		For each alert: load, filter, ingest.
		"""

		# Part 1: Setup logging 
		#######################

		self._logger.info("#######     Processing alerts     #######")
		self._accepted_alerts = []
		self._rejected_alerts = []

		run_start = time.time()
		_iter_count = 0
		tar_file = tarfile.open(tar_file_path, mode='r:gz')

		# Iterate over alerts
		for tar_info in tar_file:

			# Reach end of archive
			if tar_info is None:
				self._logger.info("Reached end of tar file %s" % tar_file_path)
				tar_file.close()
				return 

			if not tar_info.isfile():
				continue # Ignore non-file entries

			# deserialize extracted alert content
			alert_content = self._deserialize(
				tar_file.extractfile(tar_info)
			)

			# filter alert
			self._filter(
				# Create AmpelAlert instance
				AmpelAlert(alert_content['objectId'], *self._shape(alert_content))
			)

			if _iter_count == iter_max:
				self._logger.info("Reached max number of iterations")
				break

			_iter_count += 1

		self._logger.info(
			"%i alert(s) processed (time required: %is)" % 
			(_iter_count, int(time.time() - run_start))
		)

		# Return number of processed alerts
		return _iter_count


	def re_run(self, list_of_alerts, iter_max=5000):
		"""
		For each alert: load, filter, ingest.
		"""

		self._logger.info("#######     Processing alerts     #######")
		self._accepted_alerts = []
		self._rejected_alerts = []

		run_start = time.time()
		_iter_count = 0

		# Iterate over alerts
		for ampel_alert in list_of_alerts:

			# filter alert
			self._filter(ampel_alert)

			if _iter_count == iter_max:
				self._logger.info("Reached max number of iterations")
				break

			_iter_count += 1


		self._logger.info(
			"%i alert(s) processed (time required: %is)" % 
			(_iter_count, int(time.time() - run_start))
		)

		# Return number of processed alerts
		return _iter_count


	def _filter(self, ampel_alert):
		""" """
		if self._alert_filter.apply(ampel_alert) is not None:
			self._logger.info(
				"+ Ingesting %i (objectId: %s)" % 
				(ampel_alert.pps[0]['candid'], ampel_alert.tran_id)
			)
			self._accepted_alerts.append(ampel_alert)
		else:
			self._logger.info(
				"- Rejecting %i (objectId: %s)" % 
				(ampel_alert.pps[0]['candid'], ampel_alert.tran_id)
			)
			self._rejected_alerts.append(ampel_alert)


	def _deserialize(self, f):
		""" """
		reader = fastavro.reader(f)
		return next(reader, None)


	def _shape(self, alert_content):
		""" """
		if 'prv_candidates' in alert_content:
			pps = [el for el in alert_content['prv_candidates'] if el.get('candid') is not None]
			pps.insert(0,  alert_content['candidate'])
			return pps, [el for el in alert_content['prv_candidates'] if el.get('candid') is None]
		else:
			return [alert_content['candidate']], None
