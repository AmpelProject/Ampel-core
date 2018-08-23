#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Task.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 04.08.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.base.TransientView import TransientView
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler

class T3Task:
	"""
	Instances of this class are typically created by the method create_tasks
	of an instance of ampel.pipeline.t3.T3TaskBody
	"""

	def __init__(self, t3_task_config, channels, logger, global_info=None):
		"""
		:param t3_task_config: instance of child class of ampel.pipeline.t3.T3TaskConfig
		:param channels: channel(s) sub-selection: string or list of strings
		:param logger: logger from python module 'logging'
		:param global_info: optional dict instance containing info such as:
			* last time the associated job was run
			* number of alerts processed since
		"""

		self.task_config = t3_task_config
		self.channels = channels
		self.logger = logger
		self.db_logging_handler = None
		self.fed_tran_ids = []
		self.journal_notes = {}

		for handler in logger.handlers:
			if isinstance(handler, DBLoggingHandler):
				self.db_logging_handler = handler

		# Instanciate t3 unit
		self.t3_instance = t3_task_config.t3_unit_class(
			logger, 
			t3_task_config.t3_resources,
			t3_task_config.t3_unit_run_config,
			global_info
		)

		# Feedback
		LoggingUtils.propagate_log(
			logger, logging.INFO,
			"%s: task instantiated" % 
			t3_task_config.log_header
		)


	def get_config(self, param):
		"""
		param: string
		"""
		return self.task_config.get(param)


	def update(self, tran_register):
		"""
		tran_register: dict instance.
			key: transient id
			value: instance of ampel.pipeline.t3.TransientData
		"""

		# Build specific array of ampel TransientView instances where each transient 
		# is cut down according to the specified sub-selections parameters
		tran_views = []

		# Append channel info to upcoming DB logging entries
		if self.db_logging_handler:
			self.db_logging_handler.set_channels(self.channels)

		for tran_id, tran_data in tran_register.items():

			# Create transientView for specified channels using transient data
			tran_view = tran_data.create_view(self.channels, self.task_config.t2_ids)

			# No view exists for the given channel(s)
			if tran_view is None:
				continue

			# Append tran_id to the next DB logging entry
			if self.db_logging_handler:
				self.db_logging_handler.set_tranId(tran_id)

			# Feedback
			self.logger.debug(
				"TransientView created: ID: %s, CN: %s, %s" % 
				(tran_id, self.channels, TransientView.content_summary(tran_view))
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
			self.db_logging_handler.unset_tranId()

		# Feedback
		LoggingUtils.propagate_log(
			self.logger, logging.INFO,
			"Providing %s with %i TransientViews" % 
			(self.t3_instance.__class__.__name__, len(tran_register))
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
		LoggingUtils.propagate_log(
			self.logger, logging.INFO,
			"%s: calling done()" % self.task_config.log_header
		)

		# Execute method done() of associated t3 instance
		return self.t3_instance.done()
