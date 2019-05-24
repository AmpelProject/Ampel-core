#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Executor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.05.2019
# Last Modified Date: 24.05.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources, math, logging, sys, importlib, re
from time import time
from types import MappingProxyType

from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.T2RunStates import T2RunStates
from ampel.core.flags.LogRecordFlag import LogRecordFlag
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.db.DBUtils import DBUtils
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader


class T2Executor:
	"""
	TODO: submit UpdateOne operations in batch ?
	"""

	# Dict saving t2 classes. 
	# Key: unit name. Value: unit class
	t2_unit_classes = {}
	versions = {}
	
	def __init__(
		self, t2_units=None, run_state=T2RunStates.TO_RUN, 
		log_level=logging.DEBUG, schedule_tag=None, 
		update_beacon=True
	): 
		"""
		:param t2_units: ids of the t2 units to run. 
		If not specified, any t2 unit will be run
		:type t2_units: List[str]
		:param T2RunStates run_state: one ampel.core.flags.T2RunStates int value 
		(for example: T0_RUN)
		:param int log_level: logging level 
		(ex: logging.DEBUG, logging.INFO, ...)
		:param str schedule_tag: tag associated with job scheduled by T2Controller
		using the module schedule. This tag can be used to cancel the associated 
		scheduled entry.
		:param bool update_beacon: whether to update the beacon collection
		when no new t2 docs were found
		"""

		self.schedule_tag = schedule_tag

		# Get logger 
		self.logger = AmpelLogger.get_unique_logger(
			log_level=log_level
		)

		# Dict saving run config dict instances loaded from the ampel 't2_run_config' DB collection
		# Key: run config id(unit name + "_" + run_config name), Value: dict instance
		# Key example: POLYFIT_default
		self.t2_run_config = {}

		# run_state must be an int value (see ampel.core.flags.T2RunStates).
		# Only the documents matching this run_state will be processed (for example: TO_RUN)
		self.run_state = run_state

		# Prepare DB query dict
		self.query = {
			"alDocType": AlDocType.T2RECORD,
			"runState": self.run_state.value
		}

		# list of string id of the t2 units to run. If None, any t2 unit will be run
		self.required_unit_names = t2_units

		# Update query accordingly
		if t2_units:
			if len(t2_units) == 1:
				self.query['t2UnitId'] = t2_units[0]
			else:
				self.query['t2UnitId'] = {
					'$in': t2_units
				}

		# Shortcut
		self.col_blend = AmpelDB.get_collection('blend')
		self.col_tran = AmpelDB.get_collection('tran')

		if update_beacon:
		
			self.col_beacon = AmpelDB.get_collection('beacon')

			# Create t2Executor beacon doc if it does not exist yet
			self.col_beacon.update_one(
				{'_id': "t2Executors"},
				{'$set': {'_id': "t2Executors"}},
				upsert=True
			)

			if len(t2_units) == 1:
				buids = t2_units[0]
			elif isinstance(t2_units, str):
				buids = t2_units
			elif t2_units is None:
				buids = None
			else:
				buids = sorted(t2_units)

			self.beacon_id = DBUtils.b2_hash(
				"%s %s" % (buids, run_state)
			)

			# Create specific beacon doc if it does not exist yet
			if not self.col_beacon.find(
				{
					'_id': 't2Executors', 
					'beacons.id': self.beacon_id
				}
			).count():

				self.col_beacon.update_one(
					{'_id': 't2Executors'},
					{
						'$push': {
							'beacons': {
								'id': self.beacon_id,
								't2UnitId': buids,
								'runState': run_state.value,
							}
						}
					}
				)


	def _fetch_new_docs(self):
		"""
		Iterate over T2 documents, 
		setting the runState of each to RUNNING
		as it is yielded
		"""

		doc = 0
		while doc is not None:

			# get t2 document (runState is usually TO_RUN or TO_RUN_PRIO)
			doc = self.col_blend.find_one_and_update(
			    self.query, 
				{
					'$set': {
						# pylint: disable=no-member
						'runState': T2RunStates.RUNNING.value
					}
				}
			)

			yield doc


	def process_docs(
		self, cursor=None, doc_limit=None, extra_log_flags=0
	):
		"""
		:param cursor: mongodb cursor
		:param int doc_limit: max number of t2 docs to process in the loop
		:param LogRecordFlag extra_log_flags: 
			By default the LogRecordFlag CORE and T2 are used.
			Set an extra log flag here if so wished (for ex LogRecordFlag.SCHEDULED_RUN)
			or set  0 if you don't want any extra log flags
		:returns: number of t2 docs processed
		"""

		if cursor is None:
			cursor = self._fetch_new_docs()

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			LogRecordFlag.CORE|LogRecordFlag.T2|extra_log_flags
		)

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)

		# we instantiate t2 unit only once per check interval.
		# The dict t2_instance stores those instances so that these can 
		# be re-used int the while loop below
		t2_instance = {}
		t2_content_loader = {}

		counter = 0

		# Process t2_docs until next() returns None (break condition below)
		while True: 

			# Retrieve newly created T2 docs
			t2_doc = next(cursor, None)

			# No new T2 doc with the given run state
			if t2_doc is None:
				break	

			counter += 1

			# Shortcut
			t2_unit_id = t2_doc['t2UnitId']

			# Check if T2 instance exists in this run
			if not t2_unit_id in t2_instance:

				# Get T2 class and content loader class
				T2Unit, ContentLoader = self.load_t2_classes(t2_unit_id, self.logger)

				# Load resources
				resources = {
					k: AmpelConfig.get_config(
						'resources.{}'.format(k)
					)
					for k in getattr(T2Unit, 'resources')
				}

				# Instantiate T2 class
				t2_instance[t2_unit_id] = T2Unit(
					self.logger, resources
				)

				t2_content_loader[t2_unit_id] = ContentLoader(
					logger=self.logger
				)

			# Build run config id (example: )
			run_config_id = "%s_%s" % (t2_unit_id, t2_doc['runConfig'])

			# if run_config was not loaded not previously done
			if not run_config_id in self.t2_run_config:

				# Load it
				self.t2_run_config[run_config_id] = T2Executor.load_run_config(
					run_config_id, self.logger
				)

				# add run_config version info for jobreporter
				self.add_version(
					t2_unit_id, 'run_config', 
					self.t2_run_config[run_config_id]
				)

			# Load ampel.base.LightCurve instance
			t2_payload = t2_content_loader[t2_unit_id].load(t2_doc)
			assert t2_payload is not None

			# Run t2
			before_run = time()
			try:
				ret = t2_instance[t2_unit_id].run(
					t2_payload, 
					self.t2_run_config[run_config_id]['parameters'].copy()
					if self.t2_run_config[run_config_id] is not None else None
				)
			except Exception as e:
				# Record any uncaught exceptions in troubles collection.
				ret = T2RunStates.EXCEPTION
				LoggingUtils.report_exception(
					self.logger, e, tier=2, 
					run_id=db_logging_handler.get_run_id(), 
					info={
						'unit': t2_unit_id,
						'runConfig': run_config_id,
						't2Doc': t2_doc['_id']
					}
				)

			# Used as timestamp and to compute duration below (using before_run)
			now = time()
			inow = int(now)

			try: 

				# T2 units can return a T2RunStates flag rather than a dict instance
				# for example: T2RunStates.EXCEPTION, T2RunStates.BAD_CONFIG, ...
				if isinstance(ret, T2RunStates):

					self.logger.error("T2 unit returned %s" % ret)
					self.col_blend.update_one(
						{
							"_id": t2_doc['_id']
						},
						{
							'$push': {
								"results": {
									'versions': self.versions[t2_unit_id],
									'dt': inow,
									'duration': round(now - before_run,3),
									'runId': db_logging_handler.get_run_id(),
									'error': ret.value
								}
							},
							'$set': {
								'runState': ret.value
							}
						}
					)

				else:

					self.logger.debug("Saving dict returned by T2 unit")
					self.col_blend.update_one(
						{
							"_id": t2_doc['_id']
						},
						{
							'$push': {
								"results": {
									'versions': self.versions[t2_unit_id],
									'dt': inow,
									'duration': round(now - before_run,3),
									'runId': db_logging_handler.get_run_id(),
									'output': ret
								}
							},
							'$set': {
								# pylint: disable=no-member
								'runState': T2RunStates.COMPLETED.value
							}
						}
					)

			except Exception as e: 

				# TODO add error tag to Job and Transient
				LoggingUtils.report_exception(
					self.logger, e, tier=2, 
					run_id=db_logging_handler.get_run_id(), 
					info={
						't2UnitId': t2_unit_id, 
						'tranId': t2_doc['tranId']
					}
				)

			try: 

				self.col_tran.update_one(
					{"_id": t2_doc['tranId']},
					{
						"$max": {
							"modified.%s" % chan: inow 
							for chan in t2_doc['channels']
						},
						"$push": {
							"journal": {
								'tier': 2,
								'dt': inow,
								'unit': t2_unit_id,
								'success': int(not isinstance(ret, T2RunStates)),
								'channels': t2_doc['channels'],
								'runId': db_logging_handler.get_run_id()
							}
						}
					}
				)

			except Exception as e: 

				# TODO add error flag to Job and Transient
				LoggingUtils.report_exception(
					self.logger, e, tier=2, 
					run_id=db_logging_handler.get_run_id(), 
					info={
						't2UnitId': t2_unit_id, 
						'tranId': t2_doc['tranId']
					}
				)

			# Check possibly defined doc_limit
			if doc_limit and counter > doc_limit:
				break

		# Remove DB logging handler
		db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)

		if not counter and hasattr(self, "beacon_id"):
			self.update_beacon()

		return counter


	def update_beacon(self):
		"""
		"""
		self.col_beacon.update_one(
			{
				'_id': "t2Executors", 
				'beacons.id': self.beacon_id
			},
			{
				'$set': {
					'beacons.$.dt': int(time())
				}
			}
		)


	@staticmethod
	def load_run_config(run_config_id, logger=None):
		"""
		:param str run_config_id: 
			Usually: unit name + "_" + run config name, 
			for example: "POLYFIT_default"
		:returns: dict
		"""

		# Get T2 run config doc from ampel config DB
		t2_run_config_doc = AmpelConfig.get_config(
			"t2RunConfig.{}".format(run_config_id)
		)

		# Robustness
		if t2_run_config_doc is None:

			if logger:
				logger.debug(
					"Cound not find t2 run config doc with id %s" %
					run_config_id
				)

			return None

		return t2_run_config_doc


	@classmethod
	def load_t2_classes(cls, unit_name, logger=None):
		"""	
		:param str unit_name: t2 unit name
		:param AmpelLogger logger: optional logger instance
		:returns: tuple of t2 class and t2 content loader class object

		Loads a T2 unit and content loader classes using information 
		loaded from the ampel config.
		This method updates the static variable self.t2_Units 
		"""	

		# Return already loaded classes if avail
		if unit_name in cls.t2_unit_classes:
			return cls.t2_unit_classes[unit_name]

		if logger:
			logger.debug("Loading T2 unit: %s" % unit_name)

		resource = next(
			pkg_resources.iter_entry_points(
				'ampel.pipeline.t2.units', unit_name
			), 
			None
		)

		if resource is None:
			raise ValueError("Unknown T2 unit: %s" % unit_name)

		T2Unit = resource.resolve()

		if not issubclass(T2Unit, AbsT2Unit):
			raise TypeError(
				"T2 unit {} from {} is not a subclass of AbsT2Unit".format(
					T2Unit.__name__, resource.dist
				)
			)

		cls.add_version(unit_name, "py", T2Unit)

		# Load content loader associated with T2 unit 
		# (ex: LightCurveLoader for T2SNCOSMO)
		cl_module_path = getattr(T2Unit, 'content_loader')

		# get module
		content_loader_module = importlib.import_module(
			cl_module_path
		)

		# get class object
		LoaderClass = getattr(
			content_loader_module, 
			re.sub(".*\.", "", cl_module_path)
		)

		cls.t2_unit_classes[unit_name] = (T2Unit, LoaderClass)

		return T2Unit, LoaderClass


	@classmethod
	def add_version(cls, unit_name, key, arg):
		""" """
		if not unit_name in cls.versions:
			cls.versions[unit_name] = {}
		if arg is None:
			return
		elif isinstance(arg, dict) or isinstance(arg, MappingProxyType):
			if 'version' in arg:
				cls.versions[unit_name][key] = arg['version']
		elif issubclass(arg, AbsT2Unit):
			cls.versions[unit_name][key] = arg.version
