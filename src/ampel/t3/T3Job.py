#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t3/T3Job.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 22.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from time import time
from ampel.core.flags.LogRecordFlag import LogRecordFlag
from ampel.logging.DBLoggingHandler import DBLoggingHandler
from ampel.logging.DBEventDoc import DBEventDoc
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.common.AmpelUtils import AmpelUtils
from ampel.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.config.t3.LogicSchemaUtils import LogicSchemaUtils
from ampel.config.t3.LogicSchemaIterator import LogicSchemaIterator
from ampel.t3.T3JournalUpdater import T3JournalUpdater
from ampel.t3.T3Event import T3Event
from ampel.db.AmpelDB import AmpelDB


class T3Job(T3Event):
	"""
	"""

	def __init__(self, config, logger=None, **kwargs):
		""" 
		:param config: instance of :obj:`T3JobConfig <ampel.config.t3.T3JobConfig>`

		:param Logger logger:\n
			- If None, a new logger associated with a DBLoggingHandler will be created, \
			which means a new document will be inserted into the 'events' collection.
			- If you provide a logger, please note that it will NOT be changed in any way, \
			in particular, no DBLoggingHandler will be added so that no DB logging will occur.
		"""
		
		super().__init__(config, logger=logger, **kwargs)
		self.event_docs = {}
		self.run_ids = {}
		self.t3_units = {}

		# $forEach' channel operator
		if config.tasks[0].get("transients.select.channels.anyOf") == ["$forEach"]:
	
			# list all channels found in matched transients.
			chans = self._get_channels()
	
			# There is no channel-less transient (no channel == no transient)
			if chans is None:
				self.logger.info("No matching transient")
				self.no_run = True
				config.tasks.clear()
				return

			self.logger.info(
				"$forEach operator will be applied", 
				extra={'channels': chans}
			)

			new_tasks = []

			for task_config in config.tasks:
				for chan_name in chans:
					new_task = task_config.copy(deep=True)
					new_task.transients.select.channels = chan_name
					new_task.task = new_task.task + "_" + str(chan_name)
					new_tasks.append(new_task)

			# Update tasks with those generated 
			config.tasks = new_tasks
	
		if len(config.tasks) > 1:

			# Instantiate T3 units
			tasks = []
			for i, task_config in enumerate(config.tasks):
				
				# Instanciate t3 unit
				T3Unit = AmpelUnitLoader.get_class(
					tier=3, unit_name=task_config.unitId
				)
	
				# Create logger
				logger = self._create_task_logger(task_config, str(i))

				# Quieten logger if so wished
				if not self.full_console_logging:
					logger.quieten_console()
	
				# T3Event parameter db_logging is True (default)
				if hasattr(self, "db_logging_handler"):

					forked_handler = self.db_logging_handler.fork(
						LogRecordFlag.T3 | LogRecordFlag.TASK
					)

					if self.update_tran_journal:
						self.run_ids[task_config.task] = self.db_logging_handler.get_run_id()

					logger.addHandler(
						self.db_logging_handler.fork(LogRecordFlag.T3 | LogRecordFlag.TASK)
					)

				else:

					logger.warning("DB-less logger", extra={'task': task_config.task})

					if self.update_tran_journal:
						logger.warning("journal updates will not reference runId!")
	
				try:
					# Instantiate t3 unit
					self.t3_units[task_config.task] = T3Unit(
						logger, AmpelUnitLoader.get_resources(T3Unit),
						task_config.runConfig, self.global_info
					)
				except Exception as e:

					if self.raise_exc:
						raise e

					LoggingUtils.report_exception(
						self.logger, e, tier=3, info={
							'job': self.name,
							'runId':  self.run_id,
						}
					)
					continue

				if self.update_events:
					# Create event document for each task
					self.event_docs[task_config.task] = DBEventDoc(
						task_config.task, tier=3
					)

				tasks.append(task_config)

			config.tasks = tasks

		else:

			task_config = config.tasks[0]
				
			# Instanciate t3 unit
			T3Unit = AmpelUnitLoader.get_class(
				tier=3, unit_name=task_config.unitId
			)
	
			# Instantiate t3 unit
			self.t3_units[task_config.task] = T3Unit(
				self.logger, AmpelUnitLoader.get_resources(T3Unit),
				task_config.runConfig, self.global_info
			)

			if self.update_events:
				# Create event document 
				self.event_docs[task_config.task] = DBEventDoc(
					task_config.task, tier=3
				)


		if self.update_tran_journal:
			self.journal_updater = T3JournalUpdater(
				self.run_id, self.name, self.logger, self.raise_exc
			)


	def _create_task_logger(self, task_config, name_suffix=""):
		""" """
		return AmpelLogger.get_logger(
			name = task_config.task + name_suffix,
			channels = AmpelUtils.try_reduce(
				list(
					LogicSchemaUtils.reduce_to_set(
						task_config.transients.select.channels
					)
				) if task_config.get("transients.select.channels") else None
			)
		)


	def _get_channels(self):
		"""
		Method used for T3 Jobs configured with channels: $forEach

		:returns: a list of all channels found in the matched transients. \
		or None if no matching transient exists. \
		The list does not contain duplicates.
		:rtype: list(str), None
		"""

		query_res = next(
			AmpelDB.get_collection('register').aggregate(
				[
					{'$match': self._get_match_criteria()},
					{"$unwind": "$channels"},
					{
						"$group": {
		  					"_id": None,
		        			"channels": {
								"$addToSet": "$channels"
							}
						}
					}
				]
			),
			None
		)

		if query_res is None:
			return None

		return query_res['channels']


	def process_tran_data(self, transients):
		"""
		:param transients: TransientData iterable (dict_values in most cases)
		"""

		if transients is None:
			raise ValueError("Parameter transients is None")

		job_sel_conf = self.config.transients.select

		# Feed each task with transient views
		for task_config in self.config.tasks:

			task_name = task_config.task

			self.logger.info(
				"%s: processing %i TranData" % 
				(task_name, len(transients))
			)

			try:

				# shortcut
				task_sel_conf = task_config.transients.select

				#############################################################
				# NOTE: task sub-selection is in a beta state               #
				# Some complex sub-logic may not work or be supported       #
				# More importantly: someone needs to find time to test this #
				# NOTE2: OneOf sub-selection currently not implemented      #
				#############################################################
		
				# Channel filter
				if task_sel_conf.channels and task_sel_conf.channels != job_sel_conf.channels:
		
					tran_selection = set()

					# Members between iterations are 'or' connected
					for el in LogicSchemaIterator(task_sel_conf.channels):
		
						# scalar
						if isinstance(el, (str, int)):
							for tran_data in transients:
								if el in tran_data.channels:
									tran_selection.add(tran_data)

						# 'and' connected elements
						elif isinstance(el, (list, tuple)):
							for tran_data in transients:
								if all(x in tran_data.channels for x in el):
									tran_selection.add(tran_data)

						else:
							raise ValueError("Unsupported channel format")
				else:
					# No channel subselection means we accept every channel
					tran_selection = set(transients)
		

				# withTags filter
				if task_sel_conf.withTags and task_sel_conf.withTags != job_sel_conf.withTags:

					# new empty set containing tran_data matching withTags 
					wt_selection = set()

					# Members between iterations are 'or' connected
					for el in LogicSchemaIterator(task_sel_conf.withTags):

						# scalar
						if isinstance(el, (str, int)):
							for tran_data in tran_selection:
								if el in tran_data.tags:
									wt_selection.add(tran_data)

						# 'and' connected elements
						elif isinstance(el, (tuple, list)):
							for tran_data in tran_selection:
								if all(x in tran_data.tags for x in el):
									tran_selection.add(tran_data)
						else:
							raise ValueError("Unsupported withTags format")

					tran_selection = tran_selection.intersection(wt_selection)
		
		
				# withoutTags filter
				if task_sel_conf.withoutTags and task_sel_conf.withoutTags != job_sel_conf.withoutTags:
		
					# Members between iterations are 'or' connected
					for el in LogicSchemaIterator(task_sel_conf.withoutTags):
		
						# scalar
						if isinstance(el, (str, int)):
							for tran_data in transients:
								if tran_data in tran_selection and el in tran_data.tags:
									tran_selection.remove(tran_data)

						# 'and' connected elements
						elif isinstance(el, (tuple, list)):
							for tran_data in transients:
								if tran_data in tran_selection and all(x in tran_data.tags for x in el):
									tran_selection.remove(tran_data)

						else:
							raise ValueError("Unsupported withoutTags format")
		

				if not tran_selection:
					self.logger.info("%s: No transients matched selection criteria" % task_name)
					return 

				# Temporary solution, please improve
				chan_set = LogicSchemaUtils.reduce_to_set(
					task_sel_conf.channels
				)

				tran_views = self.create_tran_views(
					task_name,
					tran_selection, 
					chan_set, 
					task_config.transients.content.docs,
					task_config.transients.content.t2SubSelection
				)

				# Feedback
				self.logger.shout(
					"Providing %s (task %s) with %i TransientViews" % 
					(task_config.unitId, task_name, len(tran_views))
				)

				# Compute and add task duration for each transients chunks
				start = time()

				# Adding tviews to t3_units may return JournalUpdate dataclasses
				custom_journal_entries = self.t3_units[task_name].add(tran_views)

				if self.update_events:
					self.event_docs[task_name].add_duration(time()-start)

				if self.update_tran_journal:

					chan_list = list(chan_set)

					self.journal_updater.add_default_entries(
						tran_views, chan_list, event_name=task_name, 
						run_id=self.run_ids.get(task_name)
					)

					self.journal_updater.add_custom_entries(
						custom_journal_entries, chan_list, event_name=task_name, 
						run_id=self.run_ids.get(task_name)
					)

					# Publish journal entries to DB
					self.journal_updater.flush()

			except Exception as e:

				if self.raise_exc:
					raise e

				LoggingUtils.report_exception(
					self.logger, e, tier=3, info={
						'job': self.name,
						'runId':  self.run_id,
					}
				)


	def finish(self):

		# Feed each task with transient views
		for task_config in self.config.tasks:

			try:

				task_name = task_config.task

				# Calling T3Unit closing method done()
				start = time()
				self.t3_units[task_name].done()

				if self.update_events:
					self.event_docs[task_name].add_duration(
						time() - start
					)
					self.event_docs[task_name].publish()

			except Exception as e:

				if self.raise_exc:
					raise e

				LoggingUtils.report_exception(
					self.logger, e, tier=3, run_id=self.run_id,
					info={self.event_type: self.name}
				)
