#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/AlertProcessor.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging
import importlib, time
from ampel.pipeline.t0.AmpelAlert import AmpelAlert
from ampel.pipeline.t0.AlertFlags import AlertFlags
from ampel.pipeline.common.LoggingUtils import LoggingUtils
from ampel.pipeline.common.flags.TransientFlags import TransientFlags
from ampel.pipeline.common.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.common.flags.PhotoPointFlags import PhotoPointFlags
from ampel.pipeline.common.flags.T2SchedulingFlags import T2SchedulingFlags
from ampel.pipeline.common.flags.JobFlags import JobFlags
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.common.db.DBJobReporter import DBJobReporter
from ampel.pipeline.t0.dispatchers.AmpelDispatcher import AmpelDispatcher
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader


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
	version = 0.12

	def __init__(
		self, instrument="ZTF", photopoint_source="IPAC", alert_issuer="IPAC", 
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

			if alert_issuer == "IPAC":

				# Reference to the function loading IPAC generated avro alerts
				self.alert_loading_func = ZIAlertLoader.get_flat_pps_list_from_file

				# Update AmpelAlert static alert flags
				AmpelAlert.add_class_flags(AlertFlags.ALERT_IPAC)

			# more alert_issuers may be defined later
			else:
				raise ValueError("No implementation exists for the alert issuing entity: " + alert_issuer)

			if photopoint_source == "IPAC":

				# Set AmpelAlert static alert dict keywords description
				AmpelAlert.set_pp_dict_keywords(
					self.config['global']['photoPoints']['ZTFIPAC']['dictKeywords']
				)

				# Update AmpelAlert static alert flags
				AmpelAlert.add_class_flags(AlertFlags.INST_ZTF|AlertFlags.PP_IPAC)

				# Load PipelineDispatcher with default flags
				self.load_dispatcher()

			# more alert_issuers may be defined later
			else:
				raise ValueError("No implementation exists for the photopoints source: " + photopoint_source)


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
		tf = T2SchedulingFlags(0)
		for t2_module in d_channels[channel_name]['t2Modules']:
			tf |= T2SchedulingFlags[t2_module['module']]

		self.logger.info("On match flags: " + str(tf))

		# Associate bitmask with the filter instance
		fobj.set_on_match_default_flags(tf)

		# Reference to the "apply()" function of the T0 filter (used in run())
		channel['filter_func'] = fobj.apply

		# LogRecordFlag and TransienFlag associated with the current channel
		channel['log_flag'] = LogRecordFlags[d_channels[channel_name]['flagLabel']]
		channel['transient_flag'] = TransientFlags[d_channels[channel_name]['flagLabel']]

		# Build these two log entries once and for all (outside the main loop in run())
		channel['log_accepted'] = " -> Channel '" + channel_name + "': alert passes filter criteria"
		channel['log_rejected'] = " -> Channel '" + channel_name + "': alert was rejected"

		return channel


	def set_custom_dispatcher(self, dispatcher):
		"""
			Sets the dispatcher instance to be used in the method run().
			If unspecified, a new instance of PipelineDispatcher is used
			Known dispatcher (as for Sept 2017) are:
				* t0.dispatchers.MemoryDispatcher
				* t0.dispatchers.AmpelDispatcher
		"""
		self.dispatcher = dispatcher


	def load_dispatcher(
		self, pps_default_flags = 
			PhotoPointFlags.INST_ZTF | 
			PhotoPointFlags.ALERT_IPAC | 
			PhotoPointFlags.PP_IPAC
	):

		self.logger.info("Loading AmpelDispatcher")
		self.dispatcher = AmpelDispatcher(self.mongo_client)
		# TODO: depreciated
		#self.dispatcher.set_common_flags(pps_default_flags)


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
			raise ValueError('Dispatcher instance missing')

		# Create new "job" document in the DB
		self.db_job_reporter.insert_new(self)

		# Set dispatcher jobId 	(will be inserted in the transient documents)
		self.dispatcher.set_jobId(
			self.db_job_reporter.getJobId()
		)

		# Array of JobFlags. Each element is set by each T0 channel 
		t2_scheduling_flags = [None] * len(self.t0_channels) 
		self.dispatcher.map_channel_to_transient_flag(
			[channel['transient_flag'] for channel in self.t0_channels]
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

					# Apply filter (returns None in case of rejection or flags in case of match)
					t2_scheduling_flags[i] = channel['filter_func'](alert)

					# Log feedback
					if t2_scheduling_flags[i] is not None:
						loginfo(channel['log_accepted'])
						# TODO push transient journal entry
					else:
						loginfo(channel['log_rejected'])

					# Unset channel id <-> log entries association
					dblh_unset_temp_flags(channel['log_flag'])

				if not any(t2_scheduling_flags):
					# TODO: implement AlertDisposer class ?
					self.logger.info("Disposing rejected candidates not implemented yet")
				else:
					# Dispatch alert (
					logdebug(" -> Dispatching alert")
					dispatch(trans_id, pps_list, t2_scheduling_flags)

				# Unset log entries association with transient id
				dblh_unset_tranId()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")

		duration = int(time.time()) - start_time

		self.db_job_reporter.set_duration(duration)
		loginfo("Pipeline processing completed (time required: " + str(duration) + "s)")

		self.db_log_handler.flush()
