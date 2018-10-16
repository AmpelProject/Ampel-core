#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Job.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 15.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from datetime import datetime
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.base.flags.TransientFlags import TransientFlags
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.pipeline.config.t3.LogicSchemaUtils import LogicSchemaUtils
from ampel.pipeline.t3.T3Event import T3Event


class T3Job(T3Event):
	"""
	"""

	def __init__(self, config, logger=None, **kwargs):
		""" 
		:param config: instance of :obj:`T3JobConfig <ampel.pipeline.config.t3.T3JobConfig>`

		:param Logger logger:\n
			- If None, a new logger associated with a DBLoggingHandler will be created, \
			which means a new document will be inserted into the 'events' collection.
			- If you provide a logger, please note that it will NOT be changed in any way, \
			in particular, no DBLoggingHandler will be added so that no DB logging will occur.
		"""
		
		super().__init__(config, logger=logger, **kwargs)

		# $forEach' channel operator
		if config.tasks[0].get("transients.select.channels.anyOf") == ["$forEach"]:
	
			# list all channels found in matched transients.
			chans = self._get_channels()
	
			# There is no channel-less transient (no channel == no transient)
			if chans is None:
				self.logger.info("No matching transient")
				return

			self.logger.info("$forEach operator will be applied to %s" % chans)

			new_tasks = []

			for task_config in config.tasks:
				for chan_name in chans:
					new_task = task_config.copy(deep=True)
					new_task.transients.select.channels = chan_name
					new_tasks.append(new_task)

			# Update tasks with those generated 
			config.tasks = new_tasks
	
		if len(config.tasks) > 1:

			# Instantiate T3 units
			for task_config in config.tasks:
				
				# Instanciate t3 unit
				T3Unit = AmpelUnitLoader.get_class(
					tier=3, unit_name=task_config.unitId
				)
	
				# Create logger
				logger = AmpelLogger.get_logger(
					name=task_config.task, # task name
					#channels = list(
					#	LogicSchemaUtils.reduce_to_set(
					#		task_config.transients.select.channels
					#	)
					#) if task_config.get("transients.select.channels") else None
				)

				# Quieten logger if so wished
				if not self.full_console_logging:
					logger.quieten_console()
	
				# T3Event parameter db_logging is True (default)
				if hasattr(self, "db_logging_handler"):
					logger.addHandler(self.db_logging_handler.fork())
	
				# Instantiate t3 unit
				self.t3_units[task_config.task] = T3Unit(
					logger, AmpelUnitLoader.get_resources(T3Unit),
					task_config.runConfig, self.global_info
				)

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


	def process_tran_data(self, transients):
		"""
		"""

		if transients is None:
			raise ValueError("Parameter transients is None")

		job_sel_conf = self.config.transients.select

		# Feed each task with transient views
		for task_config in self.config.tasks:

			try:

				tran_selection = {}
				task_sel_conf = task_config.transients.select
		
				# Channel filter
				if task_sel_conf.channels != job_sel_conf.channels:
		
					for el in LogicSchemaUtils.iter(task_sel_conf.channels):
		
						if type(el) is str:
							task_chan_set = {el}
						elif isinstance(el, dict):
							task_chan_set = set(el['allOf'])
						else:
							raise ValueError("Unsupported channel format")
		
						for tran_data in transients:
							if task_chan_set.issubset(tran_data.channels):
								tran_selection[tran_data.tran_id] = tran_data
		
		
				# withFlags filter
				if task_sel_conf.withFlags != job_sel_conf.withFlags:
		
					for el in LogicSchemaUtils.iter(task_sel_conf.withFlags):
		
						if type(el) is str:
							# pylint: disable=unsubscriptable-object
							with_flags = TransientFlags[el]
						elif isinstance(el, dict):
							with_flags = LogicSchemaUtils.allOf_to_enum(el, TransientFlags)
						else:
							raise ValueError("Unsupported withFlags format")
		
						for tran_id, tran_data in tran_selection.items():
							if with_flags in tran_data.flags:
								tran_selection[tran_data.tran_id] = tran_data
		
		
				# withoutFlags filter
				if task_sel_conf.withoutFlags != job_sel_conf.withoutFlags:
		
					for el in LogicSchemaUtils.iter(task_sel_conf.withoutFlags):
		
						if type(el) is str:
							# pylint: disable=unsubscriptable-object
							without_flags = TransientFlags[el]
						elif isinstance(el, dict):
							without_flags = LogicSchemaUtils.allOf_to_enum(el, TransientFlags)
						else:
							raise ValueError("Unsupported withoutFlags format")
		
						for tran_id, tran_data in tran_selection.items():
							if without_flags in tran_data.flags and tran_id in tran_selection:
								del tran_selection[tran_data.tran_id]

				chan_set = LogicSchemaUtils.reduce_to_set(
					task_sel_conf.channels
				)

				tran_views = self.create_tran_views(
					tran_selection.values(), chan_set, 
					task_config.transients.content.docs,
					task_config.transients.content.t2SubSelection
				)

				# Feedback
				self.logger.shout(
					"Providing %s (task %s) with %i TransientViews" % 
						(
							self.t3_units[task_config.task].__class__.__name__,
							task_config.task,
							len(tran_views)
						)
				)

				if self.update_tran_journal:

					chan_list = list(chan_set)

					self.journal_updater.add_default_entries(
						tran_views, chan_list, task_config.task
					)

					self.journal_updater.add_custom_entries(
						# Adding tviews to t3_units may return JournalUpdate dataclasses
						self.t3_units[task_config.task].add(tran_views),
						chan_list, task_config.task
					)

				else:
					self.t3_units[task_config.task].add(tran_views)

			except Exception as e:

				if self.raise_exc:
					raise e

				LoggingUtils.report_exception(
					self.logger, e, tier=3, info={
						'job': self.name,
						'runId':  self.run_id,
					}
				)



	def _get_channels(self):
		"""
		:returns: a list of all channels found in the matched transients. \
		or None if no matching transient exists. \
		The list does not contain duplicates.
		:rtype: list(str), None
		"""

		query_res = next(
			AmpelDB.get_collection('main').aggregate(
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
