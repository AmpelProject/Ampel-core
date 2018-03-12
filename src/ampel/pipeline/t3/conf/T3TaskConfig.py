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

class T3TaskConfig:
	"""
	"""

	# static DB collection names
	_run_config_colname = "t3_run_config"
	_t3_units_colname = "t3_units"


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
		self.t3_unit = T3UnitConfig.load(
			t3_task_doc['t3Unit'], 
			config_db[T3TaskConfig._t3_units_colname], 
			logger
		)

		# Create T3RunConfig instance
		self.run_config = T3RunConfig.load(
			t3_task_doc['t3Unit'] + "_" + t3_task_doc['runConfig'], 
			config_db[T3TaskConfig._run_config_colname], 
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
		logger.error("Failing task doc: %s" % self.task_doc)
		raise ValueError("Invalid T3 task config: "+msg)


	def has_selection(self, criteria):
		return True if hasattr(self, "sub_sel") else False


	def get_selection(self):
		"""
		Returns transient sub-selection criteria, if available
		"""
		return self.sub_sel if hasattr(self, "sub_sel") else None


	def get_t3_unit(self):
		"""
		returns an instance of T3UnitConfig (created in constructor)
		"""
		return self.t3_unit


	def get_run_config(self):
		"""
		returns an instance of T3RunConfig (created in constructor)
		"""
		return self.run_config
