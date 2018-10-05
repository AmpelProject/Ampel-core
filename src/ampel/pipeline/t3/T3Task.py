#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Task.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 03.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.base.TransientView import TransientView
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler

class T3Task:
	"""
	"""

	@classmethod
	def load_by_name(cls, job_name, task_name, logger=None):
		"""
		:param str job_name: name of the job parent of this task
		:param str task_name: name of this task
		:param Logger logger: logger instance from python module 'logging'
		:returns: an instance of ampel.pipeline.t3.T3TaskConfig
		"""

		job_dict = AmpelConfig.get_config('t3Jobs').get(job_name)

		if job_dict is None:
			raise ValueError("Job %s not found" % job_name)

		for task_dict in job_dict['tasks']:
			if task_dict['task'] == task_name:
				return cls(
					T3TaskConfig(**task_dict), 
					logger
				)

		raise ValueError("Task %s not found" % task_name)



	def __init__(self, task_config, logger, global_info=None):
		"""
		:param T3TaskConfig task_config: child instance of ampel.pipeline.config.t3.T3TaskConfig
		:param channels: optional channels override: string or list of strings
		:param Logger logger: logger instance (python module 'logging')
		:param dict global_info: optional dict containing info such as:
		* last time the associated job was run
		* number of alerts processed since
		"""

		self.task_config = task_config
		self.logger = logger
		self.db_logging_handler = None
		self.fed_tran_ids = []
		self.journal_notes = {}

		for handler in logger.handlers:
			if isinstance(handler, DBLoggingHandler):
				self.db_logging_handler = handler

		# Instanciate t3 unit
		T3Unit = AmpelUnitLoader.get_class(
			tier=3, unit_name=task_config.unitId
		)

		# Gather resources associated with this unit
		unit_resources = {
			k: AmpelConfig.get_config('resources.{}'.format(k)) 
			for k in getattr(T3Unit, 'resources', [])
		}

		self.t3_instance = T3Unit(
			logger, unit_resources, 
			task_config.runConfig, global_info
		)

		# Feedback
		logger.propagate_log(
			logging.INFO, "%s: task instantiated" % task_config.log_header
		)


	def get_config(self, param):
		"""
		param: string
		"""
		return self.task_config.get(param)


	def update(self, tran_register):
		"""
		:param dict tran_register: key: transient id, value: instance of ampel.pipeline.t3.TransientData
		"""

		# Build specific array of ampel TransientView instances where each transient 
		# is cut down according to the specified sub-selections parameters
		tran_views = []

		# Append channel info to upcoming DB logging entries
		if self.db_logging_handler:
			self.db_logging_handler.set_channels(self.task_config.channels)

		self.logger.info("~"*60)
		self.logger.info("%s: creating TransientViews" % self.task_config.log_header)

		for tran_id, tran_data in tran_register.items():

			# Create transientView for specified channels using transient data
			tran_view = tran_data.create_view(self.task_config.channels, self.task_config.t2_ids)

			# No view exists for the given channel(s)
			if tran_view is None:
				continue

			# Append tran_id to the next DB logging entry
			if self.db_logging_handler:
				self.db_logging_handler.set_tran_id(tran_id)

			# Feedback
			self.logger.debug(
				"TransientView created: ID: %s, CN: %s, %s" % 
				(tran_id, self.task_config.channels, TransientView.content_summary(tran_view))
			)

			# Save ids of created views (used later for updating transient journal)
			self.fed_tran_ids.append(tran_id)

			# save set of (possibly reduced) channels later to be included 
			# into general journal entry
			chans = frozenset(AmpelUtils.to_set(tran_view.channel))
			if chans in self.journal_notes:
				self.journal_notes[chans].append(tran_id)
			else:
				self.journal_notes[chans] = [tran_id]

			# Populate list of transient views
			tran_views.append(tran_view)

		# Unset DB logging customization
		if self.db_logging_handler:
			self.db_logging_handler.unset_tran_id()

		# Feedback
		self.logger.propagate_log(
			logging.INFO,
			"Providing %s with %i TransientViews" % 
			(self.t3_instance.__class__.__name__, len(tran_views))
		)

		# Feed T3 instance with transientViews
		self.t3_instance.add(tran_views)

		# Unset DB logging customization
		if self.db_logging_handler:
			self.db_logging_handler.unset_channels()


	def done(self):
		"""
		"""

		# Feedback
		self.logger.propagate_log(
			logging.INFO,
			"%s: calling done()" % self.task_config.log_header
		)

		# Execute method done() of associated t3 instance
		return self.t3_instance.done()
