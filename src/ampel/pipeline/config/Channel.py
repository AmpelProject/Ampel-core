#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/Channel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.03.2018
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.FlagGenerator import FlagGenerator
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.config.ZIInputParameter import ZIInputParameter
import importlib


class Channel:
	"""
	"""

	@classmethod
	def set_T2UnitIds(cls, T2UnitIds):
		"""
		Sets static variable referencing the enum flag *class* (not instance)
		listing all known T2 ids in the database (generated at runtime by FlagGenerator)
		"""
		Channel.T2UnitIds = T2UnitIds


	@classmethod
	def set_ChannelFlags(cls, ChannelFlags):
		"""
		Sets static variable referencing the enum flag *class* (not instance)
		listing all known channels in the database (generated at runtime by FlagGenerator)
		"""
		Channel.ChannelFlags = ChannelFlags


	@classmethod
	def check_class_variable(cls, name):		
		if getattr(cls, name, None) is None:
			raise ValueError(
				("%s class variable missing. Please setup the class Channel "+
				"using the classmethod 'set_%s' first") % (name, name)
			)

	channels_col_name = 'channels'
	filters_col_name = 't0_filters'


	def __init__(
		self, config_db, channel_name=None, db_doc=None, 
		t0_ready=False, gen_flags=True, logger=None
	):
		"""
		"""

		if channel_name is None and db_doc is None:
			raise ValueError("Please set either 'channel_name' or 'db_doc'")

		if channel_name is None:
			self.load_from_doc(db_doc, logger)
			self.name = db_doc['_id']
		else:
			self.load_from_db(config_db, channel_name, logger)
			self.name = channel_name

		if gen_flags is True:
			self.gen_flag()

		if t0_ready is True:
			self.ready_t0(
				config_db, 
				LoggingUtils.get_logger() if logger is None else logger
			)


	def gen_flag(self):		
		"""
		"""

		Channel.check_class_variable("ChannelFlags")
		Channel.check_class_variable("T2UnitIds")
		
		self.flag = Channel.ChannelFlags[self.name]
		self.t2_flags = None

		for el in self.t2_config:
			try:
				if self.t2_flags is None:
					self.t2_flags = Channel.T2UnitIds[el['t2Unit']]
				else:
					self.t2_flags |= Channel.T2UnitIds[el['t2Unit']]
			except KeyError:
				raise ValueError(
					("The AMPEL T2 unit '%s' referenced by the channel '%s' does not exist.\n" +
					"Please either correct the problematic entry in section 't2Compute' of channel '%s'\n" +
					"or make sure the T2 unit '%s' exists in the mongodb collection 't2_units'.") % 
					(el['t2Unit'], self.name, self.name, el['t2Unit'])
				)


	def load_from_doc(self, db_doc, logger):		
		"""
		db_doc: dict instance containing channel configrations
		"""
		self.chan_filter_doc = db_doc['t0Filter']
		self.t2_config = db_doc['t2Compute']
		self.inputs = Channel.load_channel_inputs(db_doc['input'], logger)


	def load_from_db(self, config_db, channel_name, logger):
		"""
		config_db: instance of a mongodb Database
		channel_name: value of field '_id' in channel db document.
		FYI:
		  default db: 'Ampel_config'
		  default collection: 'channels'
		"""
		cursor = config_db[Channel.channels_col_name].find(
			{'_id': channel_name}
		)

		if cursor.count() == 0:
			raise NameError("Channel '%s' not found" % channel_name)

		self.load_from_doc(
			cursor.next(), logger
		)


	def get_input(self, instrument="ZTF", alerts="IPAC"):
		"""	
		Dict path lookup shortcut function
		"""	

		if instrument+alerts in self.inputs:
			return self.inputs[instrument+alerts]

		return None
		

	def get_flag(self):
		"""	
		Return ChannelFlags instance.
		-> The class ChannelFlags is generated by ampel.flags.FlagGenerator
		-> The instance of ChannelFlags is generated using ChannelFlags[<channel name>]
		"""	
		return self.flag


	def get_t2_flags(self):
		"""	
		Returns T2UnitIds instance.
		-> The class T2UnitIds is generated by ampel.flags.FlagGenerator
		"""	
		return self.t2_flags


	def get_filter_config(self):
		"""	
		"""	
		return self.chan_filter_doc


	def set_filter_parameter(self, param_name, param_value):
		"""	
		Manualy set/add/edit filter parameters
		"""	
		self.chan_filter_doc['parameters'][param_name] = param_value


	def get_t2_run_config(self, t2_unit_name):
		"""	
		Dict path shortcut function
		"""	
		for el in self.t2_config:
			if el['t2Unit'] == t2_unit_name:
				return el['runConfig']

		return None 

	
	def get_name(self):
		return self.name


	def ready_t0(self, config_db, logger):
		"""
		'config_db': instance of pymongo Database
		"""

		filter_id = self.chan_filter_doc['id'] 
		logger.info("Loading filter: " + filter_id)

		# Lookup filter config from DB
		cursor = config_db[Channel.filters_col_name].find(
			{'_id': filter_id}
		)

		# Robustness check
		if cursor.count() == 0:
			raise NameError("Filter '%s' not found" % filter_id)

		# Retrieve filter config from DB
		filter_doc = cursor.next()

		class_full_path = filter_doc['classFullPath']
		logger.info("   Full class path: " + class_full_path)

		# Instanciate filter class associated with this channel
		module = importlib.import_module(class_full_path)
		filter_class = getattr(module, class_full_path.split(".")[-1])
		filter_instance = filter_class(
			self.t2_flags, 
			base_config = filter_doc['baseConfig'] if 'baseConfig' in filter_doc else None, 
			run_config = self.chan_filter_doc['runConfig'], 
			logger = logger
		)

		# Feedback
		logger.info("   Version: %s" % filter_instance.version)
		logger.info("   On match flags: %s" % self.t2_flags)

		# Reference to the "apply()" function of the T0 filter (used in run())
		self.filter_func = filter_instance.apply

		# LogRecordFlag and TransienFlag associated with the current channel
		# self.log_flag = LogRecordFlags[self.name]

		# Build these two log entries once and for all (outside the main loop in run())
		self.log_accepted = " -> Channel '%s': alert passes filter criteria" % self.name
		self.log_rejected = " -> Channel '%s': alert was rejected" % self.name

		if self.get_input().auto_complete():
			self.log_auto_complete = " -> Channel '%s': accepting alert (auto-complete)" % self.name


	@staticmethod
	def load_channel_inputs(db_doc, logger):

		inputs = {}
		for input_doc in db_doc:
			if input_doc['instrument'] == "ZTF" and input_doc['alerts'] == "IPAC":
				inputs["ZTFIPAC"] = ZIInputParameter(input_doc)
			else:
				logger.warn(
					"No implementation: ignoring input with intrument=%s and alerts=%s" % 
					(input_doc['instrument'], input_doc['alerts'])
				)

		return inputs

