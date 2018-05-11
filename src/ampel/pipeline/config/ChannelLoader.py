#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ChannelLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 03.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.config.Channel import Channel
from ampel.pipeline.config.T0Channel import T0Channel
import importlib
from functools import reduce


class ChannelLoader:
	"""
	"""

	_filters_col_name = 't0_filters'


	def __init__(self, config_db, source=None, tier=None):
		"""
		"""
		self.config_db = config_db
		self.source = source
		self.tier = tier

		if source is not None and tier == 0:
			self.known_t2_units = tuple(
				el["_id"] for el in config_db['t2_units'].find({})
			)


	def load_channels(self, channel_names, logger):
		"""
		"""
		logger = LoggingUtils.get_logger() if logger is None else logger

		# Robustness check
		if channel_names is not None:
			
			if type(channel_names) is str:
				channel_names = [channel_names]

			if len(channel_names) != len(set(channel_names)):
				raise ValueError("Duplicates in provided list of channel names")


		# Get channel docs from config DB
		channel_docs = list(
			self.config_db['channels'].find(
				{} if channel_names is None else {'_id': {'$in': channel_names}}
			)
		)

		# Robustness check
		if channel_names is not None and len(channel_docs) != len(channel_names):
			db_ids = {doc['_id'] for doc in channel_docs}
			for channel_name in channel_names:
				if channel_name not in db_ids:
					raise NameError("Channel '%s' not found" % channel_name)

	
		# var referencing list of channel config objects
		channels = []

		# Loop through chsnnel config documents returned from DB
		for channel_doc in channel_docs:

			if channel_doc['active'] is False:
				logger.info("Ignoring non-active channel %s" % channel_doc['_id'])
				continue

			if self.source is None or self.tier != 0:
				# Instantiate ampel.pipeline.config.Channel object
				chan = Channel(channel_doc, source=self.source) 
			else:
				chan = self._create_t0_channel(channel_doc, logger)

			channels.append(chan)

		return channels


	def _get_config(self, channel_doc, param_name):
		"""
		"""
		return reduce(
			dict.get, 
			param_name.split("."), 
			channel_doc['sources'][self.source]
		)


	
	def _create_t0_channel(self, channel_doc, logger):
		"""
		"""
		t2_units = set()

		# Check defined t2UNits for this stream
		for el in self._get_config(channel_doc, 't2Compute'):

			if not el['t2Unit'] in self.known_t2_units:
				raise ValueError(
					("The AMPEL T2 unit '%s' referenced by the channel '%s' does not exist.\n" +
					"Please either correct the problematic entry in section 't2Compute' of channel '%s'\n" +
					"or make sure the T2 unit '%s' exists in the mongodb collection 't2_units'.") % 
					(el['t2Unit'], channel_doc['_id'], channel_doc['_id'], el['t2Unit'])
				)

			# Populate set of t2 units
			t2_units.add(el['t2Unit'])

		# Get t0 filter name
		filter_id = self._get_config(channel_doc, 't0Filter.dbEntryId')
		logger.info("Loading filter: " + filter_id)

		# Lookup filter config from DB
		cursor = self.config_db[ChannelLoader._filters_col_name].find(
			{'_id': filter_id}
		)

		# Robustness check
		if cursor.count() == 0:
			raise NameError("Filter '%s' not found" % filter_id)

		# Retrieve filter config from DB
		doc_t0_filter = cursor.next()

		class_full_path = doc_t0_filter['classFullPath']
		logger.info(" -> Full class path: " + class_full_path)

		# Instantiate filter class associated with this channel
		module = importlib.import_module(class_full_path)
		filter_class = getattr(module, class_full_path.split(".")[-1])
		filter_instance = filter_class(
			t2_units, 
			base_config = doc_t0_filter['baseConfig'] if 'baseConfig' in doc_t0_filter else None,
			run_config = self._get_config(channel_doc, 't0Filter.runConfig'), 
			logger=logger
		)

		# Feedback
		logger.info(" -> Version: %s" % filter_instance.version)
		logger.info(" -> On match t2 units: %s" % t2_units)

		return T0Channel(channel_doc, self.source, filter_instance.apply, t2_units)
