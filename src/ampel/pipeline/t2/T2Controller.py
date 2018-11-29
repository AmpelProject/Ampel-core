#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.01.2018
# Last Modified Date: 26.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources, math
import logging
import sys
from time import time
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from types import MappingProxyType

from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.T2RunStates import T2RunStates
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader

class T2Controller(Schedulable):
	"""
	TODO: submit UpdateOne operations in batch ?
	"""

	# Dict saving t2 classes. 
	# Key: unit name. Value: unit class
	t2_classes = {}
	versions = {}
	
	def __init__(
		self, run_state=T2RunStates.TO_RUN, t2_units=None, 
		check_interval=10, batch_size=200, log_level=logging.DEBUG
	): 
		"""
		:param T2RunStates run_state: one on ampel.core.flags.T2RunStates int value (for example: T0_RUN)
		:param t2_units: ids of the t2 units to run. If not specified, any t2 unit will be run
		:type t2_units: List[str]
		:param int check_interval: in seconds
		:param int batch_size: 
		"""

		# Get logger 
		self.logger = AmpelLogger.get_unique_logger(log_level=log_level)

		# check interval is in seconds
		self.check_interval = check_interval

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
		if not t2_units is None:
			if len(t2_units) == 1:
				self.query['t2UnitId'] = t2_units[0]
			else:
				self.query['t2UnitId'] = {
					'$in': t2_units
				}

		# How many docs per 'job document'
		# batch_size is defined because the job log entry cannot grow above 16MB (of logs).
		self.batch_size = batch_size

		# Parent constructor
		Schedulable.__init__(self)

		# Schedule processing of t2 docs
		self.get_scheduler().every(check_interval).seconds.do(
			self.process_new_docs
		)

		# Shortcut
		self.col_beacon = AmpelDB.get_collection('beacon')
		self.col_blend = AmpelDB.get_collection('blend')
		self.col_tran = AmpelDB.get_collection('tran')

		# Create t2Controller beacon doc if it does not exist yet
		self.col_beacon.update_one(
			{'_id': "t2Controller"},
			{'$set': {'_id': "t2Controller"}},
			upsert=True
		)

	def _fetch_new_docs(self):
		"""
		Iterate over T2 documents, setting the runState of each to RUNNING
		as it is yielded
		"""
		doc = {}
		while doc is not None:
			# get t2 document (runState is usually TO_RUN or TO_RUN_PRIO)
			doc = AmpelDB.get_collection('blend').find_one_and_update(
			    self.query, {'$set': {'runState': T2RunStates.RUNNING.value}}
			)
			yield doc

	def process_new_docs(self):
		"""
		check transient database for T2 documents with the given run_state
		"""
		batch = 0
		while True:
			# Process t2_docs
			chunk = self.process_docs(self._fetch_new_docs())
			batch += chunk
			if chunk == 0:
				break

		# Update heartbeat if no result
		if batch == 0:
			self.col_beacon.update_one(
				{'_id': "t2Controller"}, 
				{'$set': {'dt': time()}}
			)


	def process_docs(self, cursor):
		"""
		cursor: mongodb cursor
		Return: nothing
		"""

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			LogRecordFlags.T2 | 
			LogRecordFlags.CORE | 
			LogRecordFlags.SCHEDULED_RUN
			# valery: fix me later
			#info={
			#	"runState": str(self.run_state.value),
			#	"t2Units": str(self.required_unit_names)
			#}
		)

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)

		# we instantiate t2 unit only once per check interval.
		# The dict t2_instances stores those instances so that these can 
		# be re-used int the while loop below
		t2_instances = {}

		# Instantiate LightCurveLoader (that returns ampel.base.LightCurve instances)
		lcl = LightCurveLoader(logger=self.logger)

		counter = 0

		# Process t2_docs until next() returns None (break condition below)
		while True: 

			# Retrieve newly created T2 docs
			t2_doc = next(cursor, None)

			# No new T2 doc with the given run state
			if t2_doc is None:
				return counter

			counter += 1

			# Shortcut
			t2_unit_id = t2_doc['t2UnitId']

			# Check if T2 instance exists in this run
			if not t2_unit_id in t2_instances:

				# Get T2 class
				unit = self.load_unit(t2_unit_id, self.logger)

				# Load resources
				resources = {
					k: AmpelConfig.get_config('resources.{}'.format(k)) 
					for k in getattr(unit, 'resources')
				}

				# Instantiate T2 class
				t2_instances[t2_unit_id] = unit(
					self.logger, resources
				)

			# Build run config id (example: )
			run_config_id = t2_unit_id + "_" + t2_doc['runConfig']

			# if run_config was not loaded not previously done
			if not run_config_id in self.t2_run_config:

				# Load it
				self.load_run_config(run_config_id)

				# add run_config version info for jobreporter
				self.add_version(
					t2_unit_id, 'run_config', self.t2_run_config[run_config_id]
				)

			# Load ampel.base.LightCurve instance
			lc = lcl.load_from_db(
				t2_doc['tranId'], t2_doc['compId']
			)
			assert lc is not None

			# Run t2
			before_run = time()
			try:
				ret = t2_instances[t2_unit_id].run(
					lc, (
						self.t2_run_config[run_config_id]['parameters'].copy()
						if self.t2_run_config[run_config_id] is not None
						else None
					)
				)
			except Exception as e:
				# Record any uncaught exceptions in troubles collection.
				ret = T2RunStates.EXCEPTION
				LoggingUtils.report_exception(
					self.logger, e, tier=2, run_id=db_logging_handler.get_run_id(), info={
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
				# TODO add error flag to Job and Transient
				LoggingUtils.report_exception(
					self.logger, e, tier=2, 
					run_id=db_logging_handler.get_run_id(), 
					info={'t2UnitId': t2_unit_id, 'tranId': t2_doc['tranId']}
				)

			try: 

				self.col_tran.update_one(
					{
						"_id": t2_doc['tranId']
					},
					{
						"$max": {
							"modified."+chan: inow for chan in t2_doc['channels']
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
					info={'t2UnitId': t2_unit_id, 'tranId': t2_doc['tranId']}
				)

			# Take batch_size reqs into consideration
			if counter > self.batch_size:
				break


		# Remove DB logging handler
		db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)
		return counter


	def load_run_config(self, run_config_id):
		"""
		run_config_id: string (unit name + "_" + run config name)
	        example: "POLYFIT_default"
		This method populates the instance variable self.t2_run_config 
		with a reference to a dict instance loaded from the database.
		(dict key is run_config_id)
		This method does not return anything.
		"""

		# Get T2 run config doc from ampel config DB
		t2_run_config_doc = AmpelConfig.get_config("t2RunConfig.{}".format(run_config_id))

		# Robustness
		if t2_run_config_doc is None:
			self.logger.debug(
				"Cound not find t2 run config doc with id %s" %
				run_config_id
			)
			self.t2_run_config[run_config_id] = None
			return 

		self.t2_run_config[run_config_id] = t2_run_config_doc


	@classmethod
	def load_unit(cls, unit_name, logger=None):
		"""	
		:param unit_name: str
		:param logger: optional logger instance (python logging module)
		Loads a T2 unit class using information loaded from the ampel config.
		This method populates the class variable self.t2_classes 
		Return code is a t2 class object
		"""	

		# Return already loaded t2 unit if avail
		if unit_name in cls.t2_classes:
			return cls.t2_classes[unit_name]

		if logger:
			logger.debug("Loading T2 unit: %s" % unit_name)

		resource = next(
			pkg_resources.iter_entry_points('ampel.pipeline.t2.units', unit_name), 
			None
		)

		if resource is None:
			raise ValueError("Unknown T2 unit: %s" % unit_name)

		class_obj = resource.resolve()
		if not issubclass(class_obj, AbsT2Unit):
			raise TypeError(
				"T2 unit {} from {} is not a subclass of AbsT2Unit".format(
					class_obj.__name__, resource.dist
				)
			)

		cls.t2_classes[unit_name] = class_obj
		cls.add_version(unit_name, "py", class_obj)

		return class_obj


	@classmethod
	def add_version(cls, unit_name, key, arg):
		"""
		"""
		if not unit_name in cls.versions:
			cls.versions[unit_name] = {}
		if arg is None:
			return
		elif isinstance(arg, dict) or isinstance(arg, MappingProxyType):
			if 'version' in arg:
				cls.versions[unit_name][key] = arg['version']
		elif issubclass(arg, AbsT2Unit):
			cls.versions[unit_name][key] = arg.version

def get_required_resources(units=None, tier=2):
	if units is None:
		units = set()
		for channel in ChannelConfigLoader.load_configurations(None, 2):
			for source in channel.sources:
				for t2 in source.t2Compute:
					units.add(t2.unitId)
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(tier, unit).resources:
			resources.add(resource)
	return resources

def run():

	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	from ampel.pipeline.config.AmpelConfig import AmpelConfig

	parser = AmpelArgumentParser()
	parser.add_argument('-v', '--verbose', default=False, action="store_true")
	parser.add_argument('--units', default=None, nargs='+', help='T2 units to run')
	parser.add_argument('--interval', default=10, type=int, help='Seconds to wait between database polls')
	parser.add_argument('--batch-size', default=200, type=int, help='Process this many T2 docs at a time')
	
	parser.require_resource('mongo', ['writer', 'logger'])
	# partially parse command line to get config
	opts, argv = parser.parse_known_args(args=[])
	parser.require_resources(*get_required_resources(opts.units))
	# parse again, filling the resource config
	opts = parser.parse_args()
	
	AmpelLogger.set_default_stream(sys.stderr)
	controller = T2Controller(
	    batch_size=opts.batch_size,
	    check_interval=opts.interval,
	    log_level=logging.DEBUG if opts.verbose else logging.INFO
	)
	if not opts.verbose:
		controller.logger.quieten_console()
	controller.process_new_docs()
	controller.run()
