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

class T3Task:
	"""
	Instances of this class are typically created by the method create_tasks
	of an instance of ampel.pipeline.t3.T3TaskBody
	"""

	def __init__(self, t3_task_config, channels, logger, global_info=None):
		"""
		t3_task_config: instance of child class of ampel.pipeline.t3.T3TaskConfig
		channels: channel(s) sub-selection: string or list of strings
		logger: logger from python module 'logging'
		global_info: optional dict instance containing info such as:
			* last time the associated job was run
			* number of alerts processed since
		"""

		self.task_config = t3_task_config
		self.channels = channels
		self.logger = logger
		self.fed_tran_ids = []
		self.journal_notes = {}

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

		# Feedback
		self.logger.info(
			"%s: adding transientViews to t3 unit" % 
			self.task_config.log_header
		)

		# Build specific array of ampel TransientView instances where each transient 
		# is cut down according to the specified sub-selections parameters
		tran_views = []

		for tran_id, tran_data in tran_register.items():

			# Create transientView for specified channels using transient data
			tran_view = tran_data.create_view(self.channels, self.task_config.t2_ids)

			# No view exists for the given channel(s)
			if tran_view is None:
				continue

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

		# Feedback
		LoggingUtils.propagate_log(
			self.logger, logging.INFO,
			"%s: adding %i transientViews to t3 unit" % 
			(self.task_config.log_header, len(tran_register))
		)

		# Feed T3 instance with transientViews
		self.t3_instance.add(tran_views)


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
