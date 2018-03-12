#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/conf/T3UnitConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.abstract.AbsT3Unit import AbsT3Unit
import importlib


class T3UnitConfig:
	"""
	"""

	# Static dict holding references to previously instanciated t3UnitConfigs
	# key = unit_id, value = t3UnitConfig instance
	loaded_unit_conf = {}


	@classmethod # Class method creating or returning previously instanciated t3UnitConfigs
	def load(cls, unit_id, mongo_col, logger=None):
		"""
		"""
		if unit_id in cls.loaded_unit_conf:
			return cls.loaded_unit_conf[unit_id]
		else:
			cls.loaded_unit_conf[unit_id] = T3UnitConfig(mongo_col, unit_id, logger)
			return cls.loaded_unit_conf[unit_id] 		


	def __init__(self, mongo_col, unit_id, logger=None):
		"""
		'mongo_col': instance of pymongo Collection
		'unit_id': id of T3 unit as defined by field '_id' in the mongo DB
		'logger': instance of logging.Logger (std python module logging)
		"""
		
		if logger is None:
			logger = LoggingUtils.get_logger()

		logger.info("Loading T3 unit: " + unit_id)
		self.unit_id = unit_id

		# Lookup t3 unit document in DB
		cursor = mongo_col.find(
			{'_id': unit_id}
		)

		# Robustness check
		if cursor.count() == 0:
			raise ValueError(
				"T3 unit %s not found" % unit_id
			)

		# Retrieve t3 unit document
		self.doc = next(cursor)

		# Robustness check
		if not 'classFullPath' in self.doc:
			raise ValueError(
				"T3 %s unit config: dict key 'classFullPath' missing" % 
				self.unit_id
			)

		# Load optional dict 'baseConfig' from document
		if 'baseConfig' in self.doc :
			self.base_config = self.doc['baseConfig']
			logger.info("   Base config: %s" % self.base_config)
		else:
			logger.info("   No base config available")

		# Create T3 class
		class_full_path = self.doc['classFullPath']
		logger.info("   Class full path: %s " % class_full_path)
		module = importlib.import_module(class_full_path)
		self.t3_class = getattr(module, class_full_path.split(".")[-1])

		if not issubclass(self.t3_class, AbsT3Unit):
			raise ValueError("T3 unit classes must inherit the abstract class 'AbsT3Unit'")


	def get_t3_class(self):
		""" """
		return self.t3_class


	def get_base_config(self):
		""" """
		return self.base_config if hasattr(self, 'base_config') else None
