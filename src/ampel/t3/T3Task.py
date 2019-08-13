#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t3/T3Task.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 11.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from time import time

from ampel.logging.DBEventDoc import DBEventDoc
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.config.t3.LogicSchemaUtils import LogicSchemaUtils
from ampel.t3.T3JournalUpdater import T3JournalUpdater
from ampel.t3.T3Event import T3Event


class T3Task(T3Event):
	"""
	"""

	def __init__(self, config, logger=None, **kwargs):
		""" 
		:param config: instance of :obj:`T3TaskConfig <ampel.pipeline.config.t3.T3TaskConfig>`

		:param Logger logger:\n
			- If None, a new logger associated with a DBLoggingHandler will be created, \
			which means a new document will be inserted into the 'events' collection.
			- If you provide a logger, please note that it will NOT be changed in any way, \
			in particular, no DBLoggingHandler will be added so that no DB logging will occur.
		"""
		
		super().__init__(config, logger=logger, **kwargs)
		self.run_ids = {}

		self.config = config
		self.channels = None
		if self.config.transients.select.channels is not None:
			self.channels = list(
				LogicSchemaUtils.reduce_to_set(
					self.config.transients.select.channels
				)
			)
			
		# Instanciate t3 unit
		T3Unit = AmpelUnitLoader.get_class(
			tier=3, unit_name=config.unitId
		)
	
		# Instantiate t3 unit
		self.t3_unit = T3Unit(
			self.logger, AmpelUnitLoader.get_resources(T3Unit),
			config.runConfig, self.global_info
		)

		# Create event document 
		self.event_doc = DBEventDoc(self.name, tier=3)

		if self.update_tran_journal:
			self.journal_updater = T3JournalUpdater(
				self.run_id, self.name, self.logger, self.raise_exc
			)


	def process_tran_data(self, transients):
		"""
		:param List[TransientData] transients:
		"""

		if transients is None:
			raise ValueError("Parameter transients is None")

		self.logger.info(
			"%s: processing %i TranData" % 
			(self.name, len(transients))
		)

		try:

			tran_views = self.create_tran_views(
				self.name, transients, self.channels,
				docs=self.config.transients.content.docs,
				t2_filter=self.config.transients.select.get_t2_query()
			)

			# Feedback
			self.logger.shout(
				"Providing %s (task %s) with %i TransientViews" % 
				(self.config.unitId, self.name, len(tran_views))
			)

			# Compute and add task duration for each transients chunks
			start = time()

			# Adding tviews to t3_units may return JournalUpdate dataclasses
			custom_journal_entries = self.t3_unit.add(tran_views)

			self.event_doc.add_duration(time()-start)

			if self.update_tran_journal:

				self.journal_updater.add_default_entries(
					tran_views, self.channels, event_name=self.name, 
					run_id=self.run_ids.get(self.name)
				)

				self.journal_updater.add_custom_entries(
					custom_journal_entries, self.channels, event_name=self.name, 
					run_id=self.run_ids.get(self.name)
				)

				# Publish journal entries to DB
				self.journal_updater.flush()

		except Exception as e:

			if self.raise_exc:
				raise e

			LoggingUtils.report_exception(
				self.logger, e, tier=3, info={
					'task': self.name,
					'runId':  self.run_id,
				}
			)


	def finish(self):

		try:

			# Calling T3Unit closing method done()
			start = time()
			self.t3_unit.done()
			self.event_doc.add_duration(time()-start)
			self.event_doc.publish()

		except Exception as e:

			if self.raise_exc:
				raise e

			LoggingUtils.report_exception(
				self.logger, e, tier=3, run_id=self.run_id,
				info={self.event_type: self.name}
			)
