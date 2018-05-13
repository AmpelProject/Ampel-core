#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AlertTarLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.05.2018
# Last Modified Date: 13.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import tarfile
from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.pipeline.logging.LoggingUtils import LoggingUtils


class AlertTarLoader(AbsAlertLoader):
	"""
	"""

	def __init__(self, tar_path=None, file_obj=None, start=0, logger=None):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.chained_tbw = None

		if file_obj is not None:
			self.tar_file = tarfile.open(fileobj=file_obj, mode='r:gz')
		elif tar_path is not None:
			self.tar_path = tar_path
			self.tar_file = tarfile.open(tar_path)
		else:
			raise ValueError("Please provide value either for 'tar_path' or 'file_obj'")

		if start != 0:
			count = 0
			for tarinfo in self.tar_file:
				count += 1
				if count < start:
					continue
			

	def get_next(self):

		tar_info = self.tar_file.next()

		if tar_info is None:
			if hasattr(self, "tar_path"):
				self.logger.info("Reached end of tar file %s" % self.tar_path)
			else:
				self.logger.info("Reached end of tar")
			return None

		if self.chained_tbw is not None:
			file_obj = self.chained_tbw.get_alert()
			if file_obj is None:
				self.chained_tbw.tar_file.close()
				self.chained_tbw = None
			else: 
				return file_obj
			
		if tar_info.isfile():

			file_obj = self.tar_file.extractfile(tar_info)

			if tar_info.name.endswith('.avro'):
				return file_obj

			elif tar_info.name.endswith('.tar.gz'):
				self.tbws = AlertTarLoader(file_obj=file_obj)
				alert = self.tbws.get_next()
				return alert if alert is not None else self.get_next()
