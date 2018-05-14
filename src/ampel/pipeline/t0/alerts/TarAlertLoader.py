#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/TarAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.05.2018
# Last Modified Date: 14.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import tarfile
from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.pipeline.logging.LoggingUtils import LoggingUtils


class TarAlertLoader(AbsAlertLoader):
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
			

	def get_files(self):

		for tar_info in self.tar_file:

			if tar_info.isfile():

				file_obj = self.tar_file.extractfile(tar_info)

				if tar_info.name.endswith('.tar.gz'):
					chained_tal = TarAlertLoader(file_obj=file_obj)
					for sub_file_obj in chained_tal.get_files():
						yield sub_file_obj
				else:
					yield file_obj

		if hasattr(self, "tar_path"):
			self.logger.info("Reached end of tar file %s" % self.tar_path)
			self.tar_file.close()
		else:
			self.logger.info("Reached end of tar")
