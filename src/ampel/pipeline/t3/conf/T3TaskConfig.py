#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/conf/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 09.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.t3.conf.T3UnitConfig import T3UnitConfig
from ampel.pipeline.t3.conf.T3RunConfig import T3RunConfig
from ampel.pipeline.config.Channel import Channel
from ampel.flags.PhotoPointFlags import PhotoPointFlags


class T3TaskConfig:
	"""
	"""

	# static DB collection names
	_colname_run_config = "t3_run_config"
	_colname_t3_units = "t3_units"
	_colname_channels = "channels"


	def __init__(self, config_db, t3_task_doc, tran_sel=None, tran_load=None, logger=None):
		"""
		"""

		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.task_doc = t3_task_doc

		# Feedback
		self.logger.info(
			"Loading T3 task %s->%s" %
			(t3_task_doc['t3Unit'], t3_task_doc['runConfig'])
		)

		# Robustness
		if not 't3Unit' in t3_task_doc:
			self.raise_ValueError(logger, 'key "t3Unit" missing')

		# Robustness
		if not 'runConfig' in t3_task_doc:
			self.raise_ValueError(logger, 'key "runConfig" missing')

		# Create T3UnitConfig instance
		self.t3_unit_config = T3UnitConfig.load(
			t3_task_doc['t3Unit'], 
			config_db[T3TaskConfig._colname_t3_units], 
			logger
		)

		# Create T3RunConfig instance
		self.run_config = T3RunConfig.load(
			t3_task_doc['t3Unit'] + "_" + t3_task_doc['runConfig'], 
			config_db[T3TaskConfig._colname_run_config], 
			logger
		)

		# Save transient sub-selection criteria if provided
		if 'subSel' in t3_task_doc:

			if tran_load is None or tran_sel is None:
				self.raise_ValueError(
					logger, 
					"task transient sub-selection is only valid if a "+
					"top-level selection exists at job level"
				)

			# Check validity of channels sub-selection
			self.check_is_subset(t3_task_doc, tran_sel, 'channel', logger)

			channel = t3_task_doc['subSel']['channel']

			# If a fork requires the filtering of photopoints 
			if (
				'docTypes' not in t3_task_doc['subSel'] or 
				"PHOTOPOINT" in t3_task_doc['subSel']['PHOTOPOINT']
			):

				# Get channel config doc 
				chan_input = next(
					config_db[T3TaskConfig._colname_channels].find(
						{'_id': channel}
					), 
					None
				)

				# Robustness
				if chan_input is None:
					self.raise_ValueError(logger, "channel %s not found" % channel)
				
				# Load channel input parameters (ZIInputParameter)
				# channel_input key can be for example "ZTFIPAC" 
				# value would be the an instance of ZIInputParameter
				channel_input = Channel.load_channel_inputs(
					chan_input['input'], logger
				)

				# Build flags used to filter photopoints
				self.pps_must_flags = PhotoPointFlags(0)
				for key in channel_input:
					self.pps_must_flags |= channel_input[key].get_pps_must_flags()


			# Check validity of t2Id sub-selection
			# No top level t2Ids selection means *all* t2Ids
			# Allowed:             main:'nosel' -> sub:'sel' 
			# Allowed:             main:'nosel' -> sub:'nosel' 
			# Allowed if subset:   main:'sel' -> sub:'other sel' 
			if (
				('t2Id' in t3_task_doc['subSel'] or 't2Ids' in t3_task_doc['subSel']) and
				('t2Id' in tran_load or 't2Ids' in tran_load)
			):
				self.check_is_subset(t3_task_doc, tran_load, 't2Id', logger)

			# Check validity of alDocType sub-selection
			# No top level alDocType selection means *all* alDocTypes
			# Allowed:             main:'nosel' -> sub:'sel' 
			# Allowed:             main:'nosel' -> sub:'nosel' 
			# Allowed if subset:   main:'sel' -> sub:'other sel' 
			if (
				('alDocType' in t3_task_doc['subSel'] or 'alDocTypes' in t3_task_doc['subSel']) and
				('alDocType' in tran_load or 'alDocTypes' in tran_load)
			):
				self.check_is_subset(t3_task_doc, tran_load, 'alDocType', logger)

			# Check validity of state sub-selection
			if 'state' in t3_task_doc['subSel']:

				# Allowed:   main:'all' -> sub:'all' 
				# Allowed:   main:'latest' -> sub:'latest' 
				# Allowed:   main:'all' -> sub:'latest' 
				# Denied:    main:'latest' -> sub:'all' 
				if tran_load['state'] != t3_task_doc['subSel']['state']:
					if tran_load['state'] == 'latest':
						self.raise_ValueError( # TODO improve error descr
							logger, 
							"invalid state sub-selection criteria: main:'latest' -> sub:'all"
						)

			# TODO: implement withFlags withoutFlags sub selection ?

			self.sub_sel = t3_task_doc['subSel']


	def check_is_subset(self, t3_task_doc, tran_params, param, logger):
		"""
		"""
		params = param + "s"

		if param in t3_task_doc['subSel'] and type(t3_task_doc['subSel'][param]) is list:
			self.raise_ValueError(logger, "Parameter %s cannot be a list" % param)

		if params in t3_task_doc['subSel'] and not type(t3_task_doc['subSel'][params]) is list:
			self.raise_ValueError(logger, "Parameter %s must be a list" % params)

		# Check validity of channels sub-selections
		# example: if 'channel' in t3_task_doc['subSel'] or 'channels' in t3_task_doc['subSel'] :
		if param in t3_task_doc['subSel'] or params in t3_task_doc['subSel']:

			task_params = (
				t3_task_doc['subSel'][params] 
				if params in t3_task_doc['subSel'] 
				else [t3_task_doc['subSel'][param]]
			)

			# Check if top level channel selection criteria was defined
			if (
				not param in tran_params and 
				not params in tran_params
			):
				self.raise_ValueError(
					logger, 
					"Task-level %s selection requires a Job-level %s selection" %
					(param, param)
				)

			job_params = (
				tran_params[params] 
				if params in tran_params 
				else [tran_params[param]]
			)
				
			# Specified channel selection must be a subset of main selection
			for task_param in task_params:
				if not task_param in job_params:
					self.raise_ValueError(
						logger, 
						"%s '%s' is not contained in the %s selection at job level" % 
						(param, task_param, param)
					)

	
	def raise_ValueError(self, logger, msg):
		"""
		"""
		logger.error("Failing task doc: %s" % self.task_doc)
		raise ValueError("Invalid T3 task config: "+msg)


	def has_selection(self, criteria):
		"""
		"""
		return True if hasattr(self, "sub_sel") else False


	def get_parameter(self, param):
		"""
		"""
		if not param in self.task_doc:
			return None

		return self.task_doc[param]


	def get_selection(self, param=None):
		"""
		Returns transient sub-selection criteria, if available
		param: string
		"""

		if not hasattr(self, "sub_sel"):
			return None

		if param is None:
			return self.sub_sel

		if param not in self.sub_sel:
			return None

		return self.sub_sel[param]


	def get_pps_must_flags(self):
		return getattr(self, "pps_must_flags", None)


	def get_t3_unit_config(self):
		"""
		returns an instance of T3UnitConfig (created in constructor)
		"""
		return self.t3_unit_config


	def get_run_config(self):
		"""
		returns an instance of T3RunConfig (created in constructor)
		"""
		return self.run_config


	def get_t3_instance(self, logger):
		"""
		returns an instance of a child class of AbsT3Unit 
		"""
		# pylint: disable=access-member-before-definition
		if hasattr(self, "t3_instance"):
			return self.t3_instance

		# Get T3 class 
		T3_class = self.t3_unit_config.get_t3_class()

		# Instanciate T3 class 
		self.t3_instance = T3_class(
			logger, self.t3_unit_config.get_base_config()
		)

		return self.t3_instance
