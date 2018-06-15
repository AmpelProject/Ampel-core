#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ChannelLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 31.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.config.Channel import Channel
from ampel.pipeline.config.T0Channel import T0Channel
import importlib
from ampel.pipeline.common.AmpelUtils import AmpelUtils

class ChannelLoader:
	"""
	"""


	def __init__(self, config, source=None, tier=None):
		"""
		"""
		self.config = config
		self.source = source
		self.tier = tier

		if source is not None and tier == 0:
			self.known_t2_units = tuple(config['t2_units'].keys())


	def load_channels(self, arg_chan_names, logger):
		"""
		"""
		logger = LoggingUtils.get_logger() if logger is None else logger

		# Robustness check
		if arg_chan_names is not None:
			
			if type(arg_chan_names) is str:
				arg_chan_names = [arg_chan_names]

			if len(arg_chan_names) != len(set(arg_chan_names)):
				raise ValueError("Duplicates in provided list of channel names")

			# Get channel docs from config DB
			channels_to_load = {
				key: value for key, value in self.config['channels'].items() if key in arg_chan_names 
			}
		else:
			channels_to_load = self.config['channels']

		# Robustness check
		if arg_chan_names is not None and len(channels_to_load) != len(arg_chan_names):
			for channel_name in arg_chan_names:
				if channel_name not in channels_to_load:
					raise NameError("Channel '%s' not found" % channel_name)

	
		# var referencing list of channel config objects
		channels = []

		# Loop through channel config documents
		for chan_name, chan_doc in channels_to_load.items():

			if chan_doc['active'] is False:
				if arg_chan_names is None:
					logger.info("Ignoring non-active channel %s" % chan_name)
					continue
				else:
					logger.info("Loading requested non-active channel %s" % chan_name)

			if self.source is None or self.tier != 0:
				# Instantiate ampel.pipeline.config.Channel object
				chan = Channel(chan_name, chan_doc, source=self.source) 
			else:
				chan = self._create_t0_channel(chan_name, chan_doc, logger)

			channels.append(chan)

		return channels


	def _get_config(self, chan_doc, param_name):
		"""
		"""
		return AmpelUtils.get_by_path(chan_doc['sources'][self.source], param_name)

	def get_required_resources(self):
		resources = set()
		for chan_doc in self.config['channels'].values():
			assert self.tier == 0
			if not chan_doc['active']:
				continue
			filter_id = self._get_config(chan_doc, 't0Filter.dbEntryId')
			doc_t0_filter = self.config['t0_filters'][filter_id]
			class_full_path = doc_t0_filter['classFullPath']
			module = importlib.import_module(class_full_path)
			filter_class = getattr(module, class_full_path.split(".")[-1])
			if hasattr(filter_class, 'resources'):
				resources.update(filter_class.resources)
		return resources
	
	def _create_t0_channel(self, chan_name, chan_doc, logger):
		"""
		"""
		t2_units = set()

		# Check defined t2UNits for this stream
		for el in self._get_config(chan_doc, 't2Compute'):

			if not el['t2Unit'] in self.known_t2_units:
				raise ValueError(
					("The AMPEL T2 unit '%s' referenced by the channel '%s' does not exist.\n" +
					"Please either correct the problematic entry in section 't2Compute' of channel '%s'\n" +
					"or make sure the T2 unit '%s' exists in the mongodb collection 't2_units'.") % 
					(el['t2Unit'], chan_name, chan_name, el['t2Unit'])
				)

			# Populate set of t2 units
			t2_units.add(el['t2Unit'])

		# Get t0 filter name
		filter_id = self._get_config(chan_doc, 't0Filter.dbEntryId')
		logger.info("Loading filter: " + filter_id)

		# Robustness check
		if filter_id not in self.config['t0_filters']:
			raise NameError("Filter '%s' not found" % filter_id)

		# Retrieve filter config from DB
		doc_t0_filter = self.config['t0_filters'][filter_id]

		class_full_path = doc_t0_filter['classFullPath']
		logger.info(" -> Full class path: " + class_full_path)

		# Instantiate filter class associated with this channel
		module = importlib.import_module(class_full_path)
		filter_class = getattr(module, class_full_path.split(".")[-1])
		base_config = {}
		if hasattr(filter_class, 'resources'):
			for k in self.config['resources']:
				if k in filter_class.resources:
					base_config[k] = self.config['resources'][k]
		filter_instance = filter_class(
			t2_units, 
			base_config = base_config,
			run_config = self._get_config(chan_doc, 't0Filter.runConfig'), 
			logger=logger
		)

		# Feedback
		logger.info(" -> Version: %s" % filter_instance.version)
		logger.info(" -> On match t2 units: %s" % t2_units)

		return T0Channel(chan_name, chan_doc, self.source, filter_instance.apply, t2_units)
