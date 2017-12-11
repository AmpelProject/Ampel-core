import logging, time
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.pipeline.t0.factories.DispatcherFactory import DispatcherFactory
from ampel.pipeline.common.flags.TransientFlags import TransientFlags
from ampel.pipeline.common.LoggingUtils import LoggingUtils
from ampel.pipeline.common.db.DBJobInfo import DBJobInfo
from ampel.pipeline.common.flags.JobFlags import JobFlags
from pymongo import MongoClient
import numpy as np
import importlib


class PipelineTester:
	""" 
		Root T0 class handling pipeline operations.

		For each alert:
			* Load the alert
			* Filter alert based on the configured filter
			* Set policies
			* Dispatch alert based on the configured dispatcher
	"""
	version = 0.1

	def __init__(self, dispatcher="Memory", update_db=False):
		"""
			Parameters:
				* filter : 
					ID of the filter to be used in the method run().
					For a list of avail filter IDs, see the docstring of set_filter_id()
				* dispatcher : 
					ID of the dispatcher to be used in the method run().
					For a list of avail dispatcher IDs, see the docstring of set_dispatcher_id()
				* update_db: 
					If True, a database event dict (containing also log entries) 
					will be created and saved into the DB each time run() is executed

		"""
		self.logger = LoggingUtils.get_console_logger()
		self.update_db = update_db
		self.dispatcher_id = dispatcher
		self.filter_id = filter

		if update_db:
			self.mongo_client = MongoClient()
			self.db_job_info = DBJobInfo(self.mongo_client)
			self.db_job_info.add_flags(JobFlags.T0)
			self.db_log_handler = LoggingUtils.attach_mongo_loghandler(self.db_job_info)


	def load_config(self):

		db = self.mongo_client.get_database("Ampel")
		col = db.get_collection("config")
		d_config = col.find({}).next()
		d_channels  = d_config["T0"]["channels"]
		self.filter_instances = np.empty(len(d_channels.keys()), dtype=object)

		for i, filter_name in enumerate(d_channels.keys()):

			self.logger.info("Setting up channel: " + filter_name)
			d_filter = d_channels[filter_name]['filter']
			d_flag = d_channels[filter_name]['flag']

			self.logger.info("Loading filter: " + d_filter['filter_class'])
			module = importlib.import_module("t0.filters."+d_filter['filter_class'])
			self.filter_instances[i] = getattr(module, d_filter['filter_class'])() 
			self.filter_instances[i].set_filter_parameters(d_filter['parameters'])

			self.logger.info("Associated default on match flags: " + str(d_flag['on_match']))
			self.filter_instances[i].set_on_match_default_flags(d_flag['on_match'])

			self.filter_instances[i].set_log_record_flag(d_flag['channel_id'])

	def set_dispatcher_id(self, dispatcher_id):
		"""
			Sets the dispatcher that will be used in the method run().
			Known dispatcher IDs (as for Sept 2017) are:
				* "Memory" (t0.dispatchers.MemoryDispatcher)
				* "Pipeline" (t0.dispatchers.PipelineDispatcher)
		"""
		self.dispatcher_id = dispatcher_id

	def set_filter_id(self, filter_id):
		"""
			Sets the filter that will be used in the method run().
			Known filter IDs (as for Sept 2017) are:
				* "NoFilter" (t0.filters.NoFilter)
				* "SN" (t0.filters.SNFilter)
				* "Random" (t0.filters.RandFilter)
				* "Neutrino" (t0.filters.NeutrinoFilter)
		"""
		self.filter_id = filter_id

	def run(self):
		"""
			For each alert:
				* Load the alert
				* Filter alert based on the configured filter
				* Set policies
				* Dispatch alert based on the configured dispatcher
		"""
		start_time = int(time.time())

		if self.update_db:
			self.db_job_info.insert_new(self)

		self.logger.info("Loading candidate dispatcher: " + self.dispatcher_id)
		self.dispatcher = DispatcherFactory.create(self.dispatcher_id, self.mongo_client)

		aflist = AlertFileList()
		aflist.set_folder("/Users/hu/Documents/ZTF/IPAC-ZTF/ztf/src/pl/avroalerts/testprod")

		flen = len(self.filter_instances)
		filter_flags = np.empty(flen, dtype=object)

		for f in iter(aflist.get_files()):

			try:

				self.logger.debug("Processing file: " + f)
				ztf_alert = ZTFAlertLoader.load(f)
	
				self.logger.info("Processing candidate with ID: " + str(ztf_alert.get_id()))
	
				if self.update_db:
					self.db_log_handler.set_candid(ztf_alert.get_id())

				for i in range(0, flen):

					filter_flags[i] = self.filter_instances[i].apply(ztf_alert)

					if has_flag(flag | TransientFlags.T0_MATCH):
						self.logger.info("  -> Candidate passes filter criteria")
						self.logger.debug("  -> Candidate flags: " + LoggingUtils.cosmetic_flags(flag))
						flags.append(flag)
					else:
						self.logger.info("  -> Candidate was rejected")
						self.logger.debug("  -> Candidate flags: "+repr(flags))
						self.logger.debug("  -> Dispatching candidate")
						self.dispatcher.dispatch(ztf_alert)

				if len(flags) != 0:
					self.logger.debug("  -> Dispatching candidate")
					self.dispatcher.dispatch(ztf_alert)

	
				if self.update_db:
					self.db_log_handler.unset_candid()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")

		duration = int(time.time()) - start_time

		self.logger.info("Pipeline processing completed (time required: " + str(duration) + "s)")

		if self.update_db:
			self.db_job_info.set_duration(duration)
			self.db_log_handler.flush()
