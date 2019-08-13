#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/alerts/FileAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 14.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.logging.AmpelLogger import AmpelLogger
import logging, io


class FileAlertLoader():


	def __init__(self, files=None, logger=None):
		""" 
		"""
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.iter_files = None
		self.files = []

		if files is not None:
			self.add_files(files)


	def add_files(self, arg):
		"""
		"""
		if type(arg) is str:
			arg = [arg]

		for fp in arg:
			self.files.append(fp)
			self.logger.debug("Adding " + str(len(fp)) + " files to the list")

		self.iter_files = iter(self.files)


	def __iter__(self):
		return self


	def __next__(self):
		""" 
		"""
		with open(next(self.iter_files), "rb") as alert_file:
			return io.BytesIO(alert_file.read())
