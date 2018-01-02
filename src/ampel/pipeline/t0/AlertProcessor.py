#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 02.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, importlib, time

from ampel.pipeline.t0.AmpelAlert import AmpelAlert
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.pipeline.t0.dispatchers.ZIAlertDispatcher import ZIAlertDispatcher

from ampel.flags.AlertFlags import AlertFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.JobFlags import JobFlags
from ampel.flags.ChannelFlags import ChannelFlags

from ampel.pipeline.common.LoggingUtils import LoggingUtils
from ampel.pipeline.common.db.DBJobReporter import DBJobReporter




class AlertProcessor:
	""" 
		Class handling T0 pipeline operations.

		For each alert, following tasks are performed:
			* Load the alert
			* Filter alert based on the configured filter
			* Set policies
			* Dispatch alert based on the configured dispatcher

		Ampel makes sure that each dictionary contains an alflags key 
	"""
	version = 0.14

	def __init__(
		self, instrument="ZTF", alert_format="IPAC", 
		load_channels=True, config_file=None, mock_db=False
	):
		"""
			Parameters:
				* filter : 
					ID of the filter to be used in the method run().
					For a list of avail filter IDs, see the docstring of set_filter_id()
				* mock_db: 
					If True, every database operation will be run by mongomock rather than pymongo 

		"""
		self.logger = LoggingUtils.get_ampel_console_logger()

		if mock_db:
			from mongomock import MongoClient
		else:
			from pymongo import MongoClient

		self.mongo_client = MongoClient()
		self.db_job_reporter = DBJobReporter(self.mongo_client, AlertProcessor.version)
		self.db_job_reporter.add_flags(JobFlags.T0)
		self.db_log_handler = LoggingUtils.add_db_log_handler(self.logger, self.db_job_reporter)

		if config_file is None:
			db = self.mongo_client.get_database("Ampel")
			col = db.get_collection("config")
			self.config = col.find({}).next()
		else:
			import json
			self.config = json.load(open(config_file))
			db = self.mongo_client.get_database("Ampel")
			db['config'].insert_one(self.config)

		if load_channels:
			self.load_channels()

		if instrument == "ZTF":

			if alert_format == "IPAC":

				# Reference to the function loading IPAC generated avro alerts
				self.alert_loading_func = ZIAlertLoader.get_flat_pps_list_from_file

				# Set static AmpelAlert alert flags
				AmpelAlert.add_class_flags(
					AlertFlags.INST_ZTF | AlertFlags.ALERT_IPAC | AlertFlags.PP_IPAC
				)

				# Set static AmpelAlert dict keywords
				AmpelAlert.set_pp_dict_keywords(
					self.config['global']['photoPoints']['ZTFIPAC']['dictKeywords']
				)
	
				# Set dispatcher metaclass
				self.dispatcher_meta = ZIAlertDispatcher

			# more alert_formats may be defined later
			else:
				raise ValueError("No implementation exists for the alert issuing entity: " + alert_format)

		# more instruments may be defined later
		else:
			raise ValueError("No implementation exists for the instrument: "+instrument)


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
		channels_ids = self.config["T0"]["channels"].keys()
		self.t0_channels = [None] * len(channels_ids)

		for i, channel_name in enumerate(channels_ids):
			self.t0_channels[i] = self.__create_channel(channel_name)


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


	def __create_channel(self, channel_name):
		"""
			Creates T0 channel dictionary.
			It contains mainly flag values and a reference 
			to the method of an instanciated T0 filter class
		"""
		# Feedback
		self.logger.info("Setting up channel: " + channel_name)

		# Shortcuts
		d_channels  = self.config["T0"]["channels"]
		d_filter = d_channels[channel_name]['filter']

		# Instanciate new channel dict
		channel = { "name": channel_name }

		# Instanciate filter class associated with this channel
		self.logger.info("Loading filter: " + d_filter['className'])
		module = importlib.import_module("ampel.pipeline.t0.filters." + d_filter['className'])
		fobj = getattr(module, d_filter['className'])()
		fobj.set_filter_parameters(d_filter['parameters'])

		# Create the enum flags that will be associated with matching transients
		# (The flags are defined in the DB and can thus be easily customized)
		tf = T2ModuleIds(0)
		for t2_module in d_channels[channel_name]['t2Modules']:
			tf |= T2ModuleIds[t2_module['module']]

		self.logger.info("On match flags: " + str(tf))

		# Associate bitmask with the filter instance
		fobj.set_on_match_default_flags(tf)

		# Reference to the "apply()" function of the T0 filter (used in run())
		channel['filter_func'] = fobj.apply

		# LogRecordFlag and TransienFlag associated with the current channel
		channel['log_flag'] = LogRecordFlags[d_channels[channel_name]['flagLabel']]
		channel['flag'] = ChannelFlags[d_channels[channel_name]['flagLabel']]

		# Build these two log entries once and for all (outside the main loop in run())
		channel['log_accepted'] = " -> Channel '" + channel_name + "': alert passes filter criteria"
		channel['log_rejected'] = " -> Channel '" + channel_name + "': alert was rejected"

		return channel


	def set_dispatcher_instance(self, dispatcher_instance):
		"""
			Sets custom dispatcher instance to be used in the method run().
			If unspecified, a new instance of ZIAlertDispatcher() is used
			Known dispatcher (as for Sept 2017) are:
				* t0.dispatchers.MemoryDispatcher
				* t0.dispatchers.ZIAlertDispatcher
		"""
		self.dispatcher = dispatcher_instance


	def load_dispatcher(self, dispatcher_meta):
		"""
			Loads a dispatcher intance using the provided metoclass
		"""
		self.logger.info("Loading %s", dispatcher_meta.__name__)
		self.dispatcher = dispatcher_meta(
			self.mongo_client, 
			self.config,
			[channel['name'] for channel in self.t0_channels]
		)


	def get_iterable_paths(self, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", extension="*.avro"):

		# Container class allowing to conveniently iterate over local avro files 
		aflist = AlertFileList()
		aflist.set_folder(base_dir)
		aflist.set_extension(extension)
		
		self.logger.info("Returning iterable for file paths in folder: %s", base_dir)
		return iter(aflist.get_files())


	def run(self, iterable):
		"""
			For each alert:
				* Load the alert
				* Filter alert and set policies for every configured channels (defined by load_config())
				* Dispatch alert based on PipelineDispatcher (default) 
				or the dispatcher instance set by the method set_dispatcher(obj)
		"""

		self.logger.info("#######     Processing alerts     #######")
		# Save current time to later evaluate how low was the pipeline processing time
		start_time = int(time.time())

		# Check if a dispatcher instance was defined
		if not hasattr(self, 'dispatcher'):
			if not hasattr(self, 'dispatcher_meta'):
				raise ValueError('Dispatcher instance and/or metaclass is/are missing')
			else:
				self.dispatcher = self.load_dispatcher(self.dispatcher_meta)


		# Create new "job" document in the DB
		self.db_job_reporter.insert_new(self)

		# Set dispatcher jobId 	(will be inserted in the transient documents)
		self.dispatcher.set_jobId(
			self.db_job_reporter.getJobId()
		)

		# Array of JobFlags. Each element is set by each T0 channel 
		scheduled_t2_modules = [None] * len(self.t0_channels) 
		self.dispatcher.set_alertproc_channel_list(
			[channel['flag'] for channel in self.t0_channels]
		)

		# python micro-optimization
		loginfo = self.logger.info
		logdebug = self.logger.debug
		dblh_set_tranId = self.db_log_handler.set_tranId
		dblh_set_temp_flags = self.db_log_handler.set_temp_flags
		dblh_unset_temp_flags = self.db_log_handler.unset_temp_flags
		dblh_unset_tranId = self.db_log_handler.unset_tranId
		alert_loading_func = self.alert_loading_func
		dispatch = self.dispatcher.dispatch

		# Iterate over alerts
		for element in iterable:

			try:
				logdebug("Processing: " + element)

				# Load avro file into python dict instance
				trans_id, pps_list = alert_loading_func(element)
				loginfo("Processing alert: " + str(trans_id))

				# AmpelAlert will create an immutable list of immutable pp dictionaries
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
				else:
					# Dispatch alert (
					logdebug(" -> Dispatching alert")
					dispatch(trans_id, pps_list, scheduled_t2_modules)

				# Unset log entries association with transient id
				dblh_unset_tranId()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")

		duration = int(time.time()) - start_time

		self.db_job_reporter.set_duration(duration)
		loginfo("Pipeline processing completed (time required: " + str(duration) + "s)")

		self.db_log_handler.flush()
