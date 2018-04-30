#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AvroDeserializer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 30.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsAlertDeserializer import AbsAlertDeserializer
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
import fastavro

class AvroDeserializer(AbsAlertDeserializer):
	"""
	"""

	def __init__(self, logger=None):
		self.logger = LoggingUtils.get_logger() if logger is None else logger


	def get_dict(self, bytes_in):
		"""	
		Returns a dict. See AbsAlertParser docstring for more info
		"""

		try:
			reader = fastavro.reader(bytes_in)
			return next(reader, None)
	
		except:
			self.logger.exception("Exception occured while loading alert")
			return None


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

