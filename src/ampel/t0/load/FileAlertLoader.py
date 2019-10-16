#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/load/FileAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 16.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from io import BytesIO
from typing import List, Union
from ampel.logging.AmpelLogger import AmpelLogger


class FileAlertLoader():


	def __init__(self, files: Union[List[str], str] = None, logger: AmpelLogger = None):
		""" 
		"""
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.iter_files = None
		self.files = []

		if files:
			self.add_files(files)


	def add_files(self, arg: Union[List[str], str]) -> None:
		"""
		"""
		if isinstance(arg, str):
			arg = [arg]

		for fp in arg:
			self.files.append(fp)
			self.logger.debug(f"Adding {len(fp)} files to the list")

		self.iter_files = iter(self.files)


	def __iter__(self):
		return self


	def __next__(self) -> BytesIO:
		""" 
		"""
		with open(next(self.iter_files), "rb") as alert_file:
			return BytesIO(alert_file.read())
