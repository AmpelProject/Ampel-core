#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, importlib, time

from ampel.pipeline.t0.AmpelAlert import AmpelAlert
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester

from ampel.flags.AlertFlags import AlertFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.JobFlags import JobFlags
from ampel.flags.ChannelFlags import ChannelFlags

from ampel.pipeline.utils.ChannelsConfig import ChannelsConfig
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBJobReporter import DBJobReporter
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.InitLogBuffer import InitLogBuffer


class AlertProcessor:
	""" 
		Class handling T0 pipeline operations.

		For each alert, following tasks are performed:
			* Load the alert
			* Filter alert based on the configured filter
			* Set policies
			* Ingest alert based on the configured ingester
	"""
	version = 0.16

	def __init__(
		self, instrument="ZTF", alert_format="IPAC", 
		load_channels=True, config_file=None, mock_db=False
	):
		"""
			Parameters:
				* mock_db: 
					If True, every database operation will be run by mongomock rather than pymongo 

		"""
		self.logger = LoggingUtils.get_console_logger(unique=True)
		self.ilb = InitLogBuffer(LogRecordFlags.T0)
		self.logger.addHandler(self.ilb)
		self.logger.info("Setting up new AlertProcessor instance")

		if mock_db:
			from mongomock import MongoClient
		else:
			from pymongo import MongoClient

		self.mongo_client = MongoClient()

		if config_file is None:
			db = self.mongo_client.get_database("Ampel")
			col = db.get_collection("config")
			self.config = col.find({}).next()
		else:
			import json
			self.config = json.load(open(config_file))
			db = self.mongo_client.get_database("Ampel")
			db['config'].insert_one(self.config)

		self.channels_config = ChannelsConfig(self.config['channels'])

		if load_channels:
			self.load_channels()

		if instrument == "ZTF":

			if alert_format == "IPAC":

				# Reference to function loading IPAC generated avro alerts
				self.alert_loading_func = ZIAlertLoader.get_flat_pps_list_from_file

				# Set static AmpelAlert alert flags
				AmpelAlert.add_class_flags(
					AlertFlags.INST_ZTF | AlertFlags.SRC_IPAC
				)

				# Set static AmpelAlert dict keywords
				AmpelAlert.set_alert_keywords(
					self.config['global']['photoPoints']['ZTFIPAC']['dictKeywords']
				)
	
				# Set ingester class
				self.ingester_class = ZIAlertIngester

			# more alert_formats may be defined later
			else:
				raise ValueError("No implementation exists for the alert issuing entity: " + alert_format)

		# more instruments may be defined later
		else:
			raise ValueError("No implementation exists for the instrument: "+instrument)

		self.logger.info("AlertProcessor initial setup completed")


	def load_config_from_file(self, file_name):
		"""
		"""
		import json
		with open(file_name, "r") as data_file:
			self.config = json.load(data_file)


	def load_channels(self):
		"""
			Loads all T0 channel configs defined in the T0 config 
		"""
		channels_ids = self.config["channels"].keys()
		self.t0_channels = [None] * len(channels_ids)

		for i, channel_name in enumerate(channels_ids):
			self.t0_channels[i] = self.__create_channel(channel_name)

		if hasattr(self, 'ingester'):
			self.active_chanlist_change = True


	def load_channel(self, channel_name):
		"""
			Loads a channel config, that will be used in the method run().
			This method can be called multiple times with different channel names.
			Known channels IDs (as for Sept 2017) are:
			"NoFilter", "SN", "Random", "Neutrino" 
		"""
		if not hasattr(self, 't0_channels'):
			self.t0_channels = []
		else:
			for channel in self.t0_channels:
				if channel["name"] == channel_name:
					self.logger.info("Channel "+channel_name+" already loaded")
					return

		self.t0_channels.append(
			self.__create_channel(channel_name)
		)

		if hasattr(self, 'ingester'):
			self.active_chanlist_change = True


	def reset_channels(self):
		"""
		"""
		self.t0_channels = []
		if hasattr(self, 'ingester'):
			self.active_chanlist_change = True


	def __create_channel(self, channel_name):
		"""
			Creates T0 channel dictionary.
			It contains mainly flag values and a reference 
			to the method of an instanciated T0 filter class
		"""
		# Feedback
		self.logger.info("Setting up channel: " + channel_name)

		# Shortcut
		d_filter = self.channels_config.get_channel_filter_config(channel_name)

		# New channel dict
		channel = {"name": channel_name}

		# Instanciate filter class associated with this channel
		self.logger.info("Loading filter: " + d_filter['classFullPath'])
		module = importlib.import_module(d_filter['classFullPath'])
		fobj = getattr(module, d_filter['classFullPath'].split(".")[-1])()
		fobj.set_filter_parameters(d_filter['parameters'])
		fobj.set_logger(self.logger)

		# Create the enum flags that will be associated with matching transients
		t2s = self.channels_config.get_channel_t2s_flag(channel_name)
		self.logger.info("On match flags: " + str(t2s))

		# Associate enum flag with the filter instance
		fobj.set_on_match_default_flags(t2s)

		# Reference to the "apply()" function of the T0 filter (used in run())
		channel['filter_func'] = fobj.apply

		# LogRecordFlag and TransienFlag associated with the current channel
		channel['log_flag'] = LogRecordFlags[self.channels_config.get_channel_flag_name(channel_name)]
		channel['flag'] = self.channels_config.get_channel_flag_instance(channel_name)

		# Build these two log entries once and for all (outside the main loop in run())
		channel['log_accepted'] = " -> Channel '" + channel_name + "': alert passes filter criteria"
		channel['log_rejected'] = " -> Channel '" + channel_name + "': alert was rejected"

		return channel


	def set_ingester_instance(self, ingester_instance):
		"""
			Sets custom ingester instance to be used in the method run().
			If unspecified, a new instance of ZIAlertIngester() is used
			Known ingester (as for Sept 2017) are:
				* t0.ingesters.MemoryIngester
				* t0.ingesters.ZIAlertIngester
		"""
		self.ingester = ingester_instance


	def load_ingester(self, ingester_class):
		"""
			Loads and returns an ingester intance using the provided metoclass
		"""
		self.logger.info("Loading %s" % ingester_class.__name__)
		return ingester_class(
			self.mongo_client, 
			self.channels_config, 
			[chan['name'] for chan in self.t0_channels],
			self.logger
		)


	def run_iterable_paths(self, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", extension="*.avro"):

		# Container class allowing to conveniently iterate over local avro files 
		aflist = AlertFileList(self.logger)
		aflist.set_folder(base_dir)
		aflist.set_extension(extension)
		
		self.logger.info("Returning iterable for file paths in folder: %s" % base_dir)
		return self.run(
			iter(aflist.get_files())
		)


	def run(self, iterable):
		"""
			For each alert:
				* Load the alert
				* Filter alert and set policies for every configured channels (defined by load_config())
				* Ingest alert based on PipelineIngester (default) 
				or the ingester instance set by the method set_ingester(obj)
		"""


		# Part 1: Setup logging 
		#######################

		self.logger.info("Executing run method")

		# Remove logger saving "headers" before job(s) 
		self.logger.removeHandler(self.ilb)

		# Create JobReporter instance
		db_job_reporter = DBJobReporter(
			self.mongo_client, JobFlags.T0
		)

		# Create new "job" document in the DB
		db_job_reporter.insert_new(
			{
				"alertProcVersion": str(self.version),
				"ingesterId": str(self.ingester_class.__class__)
			}
		)
	
		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			db_job_reporter, 
			previous_logs=self.ilb.get_logs()
		)

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)



		# Part 2: Setup divers
		######################

		self.logger.info("#######     Processing alerts     #######")

		# Save current time to later evaluate how low was the pipeline processing time
		start_time = int(time.time())


		# Check if a ingester instance was defined
		if not hasattr(self, 'ingester'):
			if not hasattr(self, 'ingester_class'):
				raise ValueError('Ingester instance and class are missing. Please provide either one.')
			else:
				self.ingester = self.load_ingester(self.ingester_class)
		else:
			if hasattr(self, 'active_chanlist_change'):
				self.ingester = self.load_ingester(self.ingester_class)
				del self.active_chanlist_change
				
		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		self.ingester.set_job_id(
			db_job_reporter.getJobId()
		)

		# Array of JobFlags. Each element is set by each T0 channel 
		scheduled_t2_modules = [None] * len(self.t0_channels) 

		# python micro-optimization
		loginfo = self.logger.info
		logdebug = self.logger.debug
		dblh_set_tranId = db_logging_handler.set_tranId
		dblh_set_temp_flags = db_logging_handler.set_temp_flags
		dblh_unset_temp_flags = db_logging_handler.unset_temp_flags
		dblh_unset_tranId = db_logging_handler.unset_tranId
		alert_loading_func = self.alert_loading_func
		ingest = self.ingester.ingest



		# Part 3: Proceed alerts
		########################

		# Iterate over alerts
		for element in iterable:

			try:
				logdebug("Processing: " + element)

				# Load avro file into python dict instance
				trans_id, pps_list = alert_loading_func(element)

				# AmpelAlert will create an immutable list of immutable pp dictionaries
				loginfo("Processing alert: " + str(trans_id))
				alert = AmpelAlert(trans_id, pps_list)

				# Associate upcoming log entries with the current transient id
				dblh_set_tranId(trans_id)

				# Loop through initialized channels
				for i, channel in enumerate(self.t0_channels):

					# Associate upcoming log entries with the current channel
					dblh_set_temp_flags(channel['log_flag'])

					# Apply filter (returns None in case of rejection or t2 modules ids in case of match)
					scheduled_t2_modules[i] = channel['filter_func'](alert)

					# Log feedback
					if scheduled_t2_modules[i] is not None:
						loginfo(channel['log_accepted'])
						# TODO push transient journal entry
					else:
						loginfo(channel['log_rejected'])

					# Unset channel id <-> log entries association
					dblh_unset_temp_flags(channel['log_flag'])

				if not any(scheduled_t2_modules):
					# TODO: implement AlertDisposer class ?
					self.logger.info("Disposing rejected candidates not implemented yet")

					if len(pps_list) > 1:
						# TODO check autocomplete set of ids !
						# for each channel from db_transient:
						# 		convert channel into i position
						#		scheduled_t2_modules[i] = default_t2ModuleIds_for_this_channel
						pass
				else:
					# Ingest alert
					logdebug(" -> Ingesting alert")
					ingest(trans_id, pps_list, scheduled_t2_modules)

				# Unset log entries association with transient id
				dblh_unset_tranId()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")


		duration = int(time.time()) - start_time
		db_job_reporter.set_duration(duration)
		self.logger.addHandler(self.ilb)
		loginfo("Pipeline processing completed (time required: " + str(duration) + "s)")

		# Remove DB logging handler
		db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)
