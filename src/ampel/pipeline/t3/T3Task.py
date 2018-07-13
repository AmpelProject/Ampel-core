#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Task.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 13.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.common.AmpelUtils import AmpelUtils

class T3Task:
	"""
	Instances of this class are typically created by the method create_tasks
	of an instance of ampel.pipeline.t3.T3TaskBody
	"""

	def __init__(self, t3_task_config, channels, logger, global_info=None):
		"""
		t3_unit_instance: instance of child class of ampel.base.abstract.AbsT3Unit
		channels: channel(s) sub-selection: string or list of strings
		t2_ids: t2 id(s) sub-selection: string or list of strings
		log_header: string
		logger: logger from python module 'logging'
		global_info: optional dict instance containing info such as:
			* last time the associated job was run
			* number of alerts processed since
		"""

		self.task_config = t3_task_config
		self.channels = channels
		self.logger = logger
		self.fed_tran_ids = []

		# Instanciate t3 unit
		self.t3_instance = t3_task_config.t3_unit_class(
			logger, 
			t3_task_config.t3_resources,
			t3_task_config.t3_unit_run_config,
			global_info
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
				"TransientView created for %s and channel(s) %s" % 
				(tran_id, self.channels)
			)
		
			# Save ids of created views (used later for updating transient journal)
			self.fed_tran_ids.append(tran_id)

			# Populate list of transient views
			tran_views.append(tran_view)

		# Feedback
		self.logger.info(
			"%s: adding %i transientViews to t3 unit" % 
			(self.task_config.log_header, len(tran_register))
		)

		# Feed T3 instance with transientViews
		self.t3_instance.add(tran_views)


	def done(self):
		"""
		"""
		self.logger.info("%s: calling done()" % self.task_config.log_header)
		return self.t3_instance.done()
