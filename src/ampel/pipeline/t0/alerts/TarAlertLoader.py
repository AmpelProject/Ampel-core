#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/TarAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.05.2018
# Last Modified Date: 17.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import tarfile
from ampel.pipeline.logging.LoggingUtils import LoggingUtils


class TarAlertLoader():
	"""
	"""

	def __init__(self, tar_path=None, file_obj=None, start=0, logger=None):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.chained_tal = None

		if file_obj is not None:
			self.tar_file = tarfile.open(fileobj=file_obj, mode='r:gz')
		elif tar_path is not None:
			self.tar_path = tar_path
			self.tar_file = tarfile.open(tar_path, mode='r:gz')
		else:
			raise ValueError("Please provide value either for 'tar_path' or 'file_obj'")

		if start != 0:
			count = 0
			for tarinfo in self.tar_file:
				count += 1
				if count < start:
					continue


	def __iter__(self):
		return self
	

	def __next__(self):
		"""
		"""
		# Free memory 
		self.tar_file.members.clear() 

		if self.chained_tal is not None:
			file_obj = self.get_chained_next()
			if file_obj is not None:
				return file_obj

		# Get next element in tar archive
		tar_info = self.tar_file.next()

		# Reach end of archive
		if tar_info is None:
			if hasattr(self, "tar_path"):
				self.logger.info("Reached end of tar file %s" % self.tar_path)
				#self.tar_file.close()
			else:
				self.logger.info("Reached end of tar")
			raise StopIteration

		# Ignore non-file entries
		if tar_info.isfile():

			# extractfile returns a file like obj
			file_obj = self.tar_file.extractfile(tar_info)

			# Handle tars with nested tars
			if tar_info.name.endswith('.tar.gz'):
				self.chained_tal = TarAlertLoader(file_obj=file_obj)
				file_obj = self.get_chained_next()
				return file_obj if file_obj is not None else next(self)
			else:
				return file_obj


	def get_chained_next(self):
		"""
		"""
		file_obj = next(self.chained_tal, None)
		if file_obj is None:
			self.chained_tal = None
			return None
		else:
			return file_obj
