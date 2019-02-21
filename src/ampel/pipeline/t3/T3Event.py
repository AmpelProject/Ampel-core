#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Event.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 11.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from time import time
from datetime import datetime
from itertools import islice
from abc import abstractmethod
from mongomock.filtering import filter_applies
from functools import partial
from typing import Optional, Dict, Any

from ampel.utils.json_serialization import AmpelEncoder
from ampel.pipeline.db.query.QueryMatchTransients import QueryMatchTransients
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.query.QueryEventsCol import QueryEventsCol
from ampel.pipeline.db.DBContentLoader import DBContentLoader
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBEventDoc import DBEventDoc
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.t3.LogicSchemaUtils import LogicSchemaUtils
from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils
from ampel.core.flags.LogRecordFlag import LogRecordFlag
from ampel.base.TransientView import TransientView
from ampel.base.dataclass.GlobalInfo import GlobalInfo


class T3Event:
	"""
	"""

	def __init__(
		self, config, logger=None, db_logging=True, 
		full_console_logging=True, update_tran_journal=True, 
		update_events=True, raise_exc=False, admin_msg=None
	):
		""" 
		Note: if you want to run an (anyomous) event without updating to the DB, please use:
		- db_logging=False (no logging into the logs collection)
		- update_tran_journal=False (no update to the transient doc)
		- update_events=False (no update to the events collection)
		- raise_exc=True (troubles collection will not be populated if an exception occurs)

		:param config: instance of :obj:`T3JobConfig <ampel.pipeline.config.t3.T3JobConfig>` \
		or :obj:`T3TaskConfig <ampel.pipeline.config.t3.T3JobConfig>`

		:param AmpelLogger logger:\n
			- If None, a new logger associated with a DBLoggingHandler will be created, \
			which means a new document will be inserted into the 'events' collection.
			- If you provide a logger, please note:
				- that it will NOT be changed in any way in particular, \
				  no DBLoggingHandler will be added so that no DB logging will occur.
				- it must be an AmpelLogger (or you could implement the method `shout` to your own logger)
		:type logger: :py:class:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>`

		:param bool full_console_logging: If False, the logging level of the streamhandler \
		associated with the logger will be set to WARN.

		:param bool update_tran_journal: Record the invocation of this event \
		in the journal of each selected transient

		:param bool update_events: Record this event in the events collection

		:param bool raise_exc: Raise exceptions instead of catching them \
		and populating 'troubles' collection

		:param str admin_msg: optional admin message to include in GlobalInfo dataclass
		"""
		
		self.tran_config = config.transients
		self.full_console_logging = full_console_logging
		self.update_tran_journal = update_tran_journal
		self.update_events = update_events
		self.raise_exc = raise_exc
		self.logger = logger
		self.config = config
		self.global_info = None
		self.run_id = None
		self.no_run = False
		self.blend_col = AmpelDB.get_collection("blend")

		if not db_logging:
			if update_tran_journal:
				raise ValueError("update_tran_journal cannot be True when db_logging is False")
			if update_events:
				raise ValueError("update_events cannot be True when db_logging is False")

		if isinstance(config, T3JobConfig):
			self.name = config.job
			self.event_type = "job"
		elif isinstance(config, T3TaskConfig):
			self.name = config.task
			self.event_type = "task"
		else:
			raise ValueError("Unknwon config %s" % type(config))

		if logger is None:

			# Create logger
			self.logger = AmpelLogger.get_logger(
				name=self.name, force_refresh=True
				#channels=list(
				#	LogicSchemaUtils.reduce_to_set(
				#		self.tran_config.select.channels
				#	)
				#) if self.tran_config.get("select.channels") else None
			)

			if db_logging:

				# Create DB logging handler instance (logging.Handler child class)
				# This class formats, saves and pushes log records into the DB
				self.db_logging_handler = DBLoggingHandler(LogRecordFlag.T3 | LogRecordFlag.CORE)

				# Add db logging handler to the logger stack of handlers 
				self.logger.addHandler(self.db_logging_handler)

				self.run_id = self.db_logging_handler.get_run_id()

			if not full_console_logging:
				self.logger.quieten_console()
			
		else:
			self.logger = logger

		# Retrieve number of alerts processed since last run or set admin msg
		if config.globalInfo or admin_msg:
			self.global_info = self.get_global_info(admin_msg)

		# T3 Event requiring prior transient loading 
		if self.tran_config is not None and self.tran_config.select is not None:
	
			# Required to get transient info
			self.db_content_loader = DBContentLoader(
				verbose=self.config.transients.verbose, 
				debug=self.config.transients.debug, 
				logger=self.logger
			)


	@abstractmethod
	def process_tran_data(self, transients):
		""" """
		pass


	@abstractmethod
	def finish(self):
		pass


	def _get_match_criteria(self):
		"""
		Returns a dict (matching criteria) used for pymongo 
		operations find() or aggregate() operations

		:returns: dict
		"""

		# Build query for matching transients using criteria defined in config
		return QueryMatchTransients.match_transients(
			channels = self.tran_config.select.channels,
			time_created = TimeConstraint(self.tran_config.select.created),
			time_modified = TimeConstraint(self.tran_config.select.modified),
			with_tags = FlagUtils.hash_schema(
				self.tran_config.select.withTags
			) if self.tran_config.select.withTags else None,
			without_tags = FlagUtils.hash_schema(
				self.tran_config.select.withoutTags
			) if self.tran_config.select.withoutTags else None
		)


	def get_global_info(self, admin_msg=None):
		"""
		Retrieves info such as the number of alerts 
		processed since last run of this event

		:returns: a GlobalInfo dataclass instance
		:rtype: :py:class:`GlobalInfo <ampel.base.dataclass.GlobalInfo>`
		"""

		# Admin messages can be forwarded to t3 units 
		if admin_msg and not self.config.globalInfo:
 			return GlobalInfo(
				**{
					'event': self.name,
					'admin_msg': admin_msg
				}
			)

		# Get date-time of last run
		last_run = AmpelUtils.get_by_path(
			next(
				AmpelDB.get_collection('events').aggregate(
					QueryEventsCol.get_last_run(self.name)
				),
				None
			),
			'events.dt'
		)

		if last_run is None:

			# Feedback
			self.logger.warning(
				"Event %s: last run time unavailable" % 
				self.name
			)

		else:

			# Get number of alerts processed since last run
			res = next(
				AmpelDB.get_collection('events').aggregate(
					QueryEventsCol.get_t0_stats(last_run)
				), 
				None
			)

		# Build and return global info
		return GlobalInfo(
			**{
				'event': self.name,
				'last_run': datetime.utcfromtimestamp(last_run),
				'processed_alerts': None if res is None else res.get('alerts'),
				'admin_msg': admin_msg
			}
		)


	def _get_selected_transients(self):
		"""
		:returns: pymongo.cursor.Cursor instance
		"""

		# Build query for matching transients using criteria defined in config
		match_query = self._get_match_criteria()
		self.logger.info(
			"Executing search query", 
			extra=LoggingUtils.safe_query_dict(match_query)
		)

		# Execute 'find transients' query
		trans_cursor = AmpelDB.get_collection('tran').find(
			match_query, {'_id':1}, # indexed query
			no_cursor_timeout=True, # allow query to live for > 10 minutes
		).hint('_id_1_channels_1')
		
		# Count results 
		if trans_cursor.count() == 0:
			self.logger.info("No transient matches the given criteria")
			return None

		self.logger.info(
			"%i transients match search criteria" % 
			trans_cursor.count()
		)

		return trans_cursor


	def _get_tran_data(self, trans_cursor, chunk_size):
		"""
		Yield selected TransientData in chunks of length `chunk_size`
		"""

		self.logger.info("Processing chunk")

		# Load ids (chunk_size number of ids)
		for chunked_tran_ids in T3Event._chunk(
			map(lambda el: el['_id'], trans_cursor), 
			chunk_size
		):

			self.logger.info("Loading %i transients " % len(chunked_tran_ids))
			state_ids = None

			# For '$latest' state, the latest compoundid of each transient must be determined
			if self.tran_config.state == "$latest":

				self.logger.info("Retrieving latest state")

				# ids for which the fast query cannot be used (results cast into set)
				slow_ids = set(
					el['tranId'] for el in self.blend_col.find(
						{
							'tranId': {
								'$in': chunked_tran_ids
							},
							'alDocType': AlDocType.COMPOUND, 
							'tier': {'$ne': 0}
						},
						{'_id':0, 'tranId':1}
					).batch_size(chunk_size)
				)

				# set of transient states (see comment below for an example)
				state_ids = set()

				# Channel/Channels must be provided if state is 'latest'
				# Get latest state ** for each channel(s) criteria **
				for chan_logic in LogicSchemaUtils.iter(self.tran_config.select.channels):

					# get latest state (fast mode) 
					# Output example:
					# [
					# {
					#   '_id': Binary(b']\xe2H\x0f(\xbf\xca\x0b\xd3\xba\xae\x89\x0c\xb2\xd2\xae', 5), 
					#   'tranId': 1810101034343026   # (ZTF18aaayyuq)
					# },
					# {
					#   '_id': Binary(b'_\xcd\xed\xa5\xe1\x16\x98\x9ai\xf6\xcb\xbd\xe7#FT', 5), 
					#   'tranId': 1810101011182029   # (ZTF18aaabikt)
					# },
					# ...
					# ]
					state_ids.update(
						[
							el['_id'] for el in self.blend_col.aggregate(
								QueryLatestCompound.fast_query(
									slow_ids.symmetric_difference(chunked_tran_ids), 
									channels=chan_logic
								)
							).batch_size(chunk_size)
						]
					)

					# TODO: check result length ?


					# get latest state (general mode) for the remaining transients
					for tran_id in slow_ids:

						# get latest state for single transients using general query
						g_latest_state = next(
							self.blend_col.aggregate(
								QueryLatestCompound.general_query(
									tran_id, project={
										'$project': {'_id':1}
									}
								)
							).batch_size(chunk_size),
							None
						)

						# Robustness
						if g_latest_state is None:
							# TODO: add error flag to transient doc ?
							# TODO: add error flag to event doc
							# TODO: add doc to Ampel_troubles
							self.logger.error(
								"Could not retrieve latest state for transient %s" % 
								tran_id
							)
							continue

						state_ids.add(g_latest_state['_id'])


			# Load ampel TransientData instances with given states
			self.logger.info("Loading transients")
			al_tran_data = self.db_content_loader.load_new(
				chunked_tran_ids, self.tran_config.select.channels, 
				self.tran_config.state, state_ids, self.tran_config.content.docs, 
				self.tran_config.content.t2SubSelection
			)
			
			yield al_tran_data

	def _matches_selection(self, view : TransientView, match : Optional[Dict[str,Any]]) -> bool:
		"""
		Match transient view against a Mongo query
		"""
		return view is not None and (match is None or filter_applies(match, AmpelEncoder(lossy=True).default(view)))

	def create_tran_views(self, event_name, transients, channels, docs=None, t2_subsel=None, t2_filter : Optional[Dict[str,Any]]=None):
		"""
		:param transients: list of TransientData instances
		:type transients: list(:py:class:`TransientData <ampel.pipeline.t3.TransientData>`)

		:rtype: list(:py:class:`TransientView <ampel.base.TransientView>`)
		"""

		# Append channel info to upcoming DB logging entries
		#if self.db_logging_handler:
		#	self.db_logging_handler.set_channels(self.task_config.channels)

		self.logger.info(
			"%s: creating TranViews for %i TranData" % 
			(event_name, len(transients))
		)

		if isinstance(channels, dict):
			channels = LogicSchemaUtils.reduce_to_set(channels)

		# Build specific array of ampel TransientView instances where each transient 
		# is cut down according to the specified sub-selections parameters
		# None means no view exists for the given channel(s)
		tran_views = tuple(filter(partial(self._matches_selection, match=t2_filter),
			[
				el.create_view(channels, docs, t2_subsel)
				for el in transients
			]
		))

		# Feedback if so wished
		if self.config.transients is not None and self.config.transients.debug:

			list_chan = AmpelUtils.to_list(channels, try_reduce=True)
			for tran_view in tran_views:
				self.logger.debug(
					"TranView created: %s" % TransientView.content_summary(tran_view),
					extra={
						'tranId': tran_view.tran_id,	
						'channels': list_chan
					}
				)

		return tran_views


	def run(self):
		"""
		"""

		time_start = time()

		try:

			# Feedback
			self.logger.shout("Running %s" % self.name)

			if self.update_events:
				event_doc = DBEventDoc(self.name, tier=3)
				event_doc.add_run_id(self.run_id)

			if self.no_run:
				self.logger.warn("Run execution not possible")
				return
		
			# T3 event requiring prior transient loading 
			if self.config.transients is not None and self.config.transients.select is not None:
	
				# Job with transient input
				trans_cursor = self._get_selected_transients()
	
				if trans_cursor is not None:
	
					# Set chunk_size to 'number of transients found' if not defined
					chunk_size = self.tran_config.chunk
	
					# No chunk size == all transients loaded at once
					if chunk_size is None:
						chunk_size = trans_cursor.count()
	
					for transients in self._get_tran_data(trans_cursor, chunk_size):
	
						try:
							self.process_tran_data(transients)
						except Exception as e:

							if self.raise_exc:
								raise e

							LoggingUtils.report_exception(
								self.logger, e, tier=3, run_id=self.run_id,
								info={self.event_type: self.name}
							)
			else:

				# Handle transient-less T3 units here
				# TODO: implement
				pass

		except Exception as e:

			if self.raise_exc:
				raise e

			LoggingUtils.report_exception(
				self.logger, e, tier=3, run_id=self.run_id,
				info={self.event_type: self.name}
			)

		finally:

			# Calls done() for each T3 unit instance (among other things)
			self.finish()

			# Register the execution of this event into the events col
			if self.update_events:
				event_doc.publish()

			# Feedback
			self.logger.shout("Done running %s" % self.name)

			# Write log entries to DB
			if hasattr(self, 'db_logging_handler'):
				self.db_logging_handler.flush_all()


	@staticmethod
	def _chunk(iter, chunk_size):
		while True:
			group = list(islice(iter, chunk_size))
			if len(group) > 0:
				yield group
			else:
				break
