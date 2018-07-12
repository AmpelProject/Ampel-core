#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ChannelLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.config.Channel import Channel
from ampel.pipeline.config.T0Channel import T0Channel
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.t2.T2Controller import T2Controller

class ChannelLoader:
	"""
	"""


	def __init__(self, source=None, tier=None, logger=None):
		"""
		"""
		self.source = source
		self.tier = tier
		self.logger = LoggingUtils.get_logger() if logger is None else logger

	def load_channels(self, arg_chan_names):
		"""
		"""

		# Robustness check
		if arg_chan_names is not None:
			
			if type(arg_chan_names) is str:
				arg_chan_names = [arg_chan_names]

			if len(arg_chan_names) != len(set(arg_chan_names)):
				raise ValueError("Duplicates in provided list of channel names")

			# Get channel docs from config DB
			channels_to_load = {
				key: value for key, value in AmpelConfig.get_config('channels').items() if key in arg_chan_names 
			}
		else:
			channels_to_load = AmpelConfig.get_config('channels')

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
					self.logger.info("Ignoring non-active channel %s" % chan_name)
					continue
				else:
					self.logger.info("Loading requested non-active channel %s" % chan_name)

			if self.source is None or self.tier != 0:
				# Instantiate ampel.pipeline.config.Channel object
				chan = Channel(chan_name, chan_doc, source=self.source) 
			else:
				chan = self._create_t0_channel(chan_name, chan_doc, self.logger)

			channels.append(chan)

		return channels


	def _get_config(self, chan_doc, param_name):
		"""
		"""
		return AmpelUtils.get_by_path(chan_doc['sources'][self.source], param_name)

	def get_required_resources(self):
		resources = set()
		for chan_doc in AmpelConfig.get_config('channels').values():
			if not chan_doc['active']:
				continue
			if self.tier == 0:
				filter_id = self._get_config(chan_doc, 't0Filter.dbEntryId')
				doc_t0_filter = AmpelConfig.get_config('t0_filters')[filter_id]
				class_full_path = doc_t0_filter['classFullPath']
				module = importlib.import_module(class_full_path)
				filter_class = getattr(module, class_full_path.split(".")[-1])
				resources.update(filter_class.resources)
			elif self.tier == 2:
				for t2_doc in self._get_config(chan_doc, 't2Compute'):
					t2_unit = T2Controller.load_unit(t2_doc['t2Unit'], self.logger)
					resources.update(t2_unit.resources)
			else:
				raise ValueError("I don't know how to discover resources for tier {}".format(self.tier))
		return resources
	
	def _create_t0_channel(self, chan_name, chan_doc, logger):
		"""
		"""
		t2_units = set()

		# Check defined t2UNits for this stream
		for el in self._get_config(chan_doc, 't2Compute'):

			try:
				T2Controller.load_unit(el['t2Unit'], logger)
			except ValueError:
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
		if filter_id not in AmpelConfig.get_config('t0_filters'):
			raise NameError("Filter '%s' not found" % filter_id)

		# Retrieve filter config from DB
		doc_t0_filter = AmpelConfig.get_config('t0_filters')[filter_id]

		class_full_path = doc_t0_filter['classFullPath']
		logger.info(" -> Full class path: " + class_full_path)

		# Instantiate filter class associated with this channel
		module = importlib.import_module(class_full_path)
		filter_class = getattr(module, class_full_path.split(".")[-1])
		base_config = {}
		if hasattr(filter_class, 'resources'):
			for k in AmpelConfig.get_config('resources'):
				if k in filter_class.resources:
					base_config[k] = AmpelConfig.get_config('resources')[k]
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
