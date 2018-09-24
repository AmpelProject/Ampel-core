#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/ChannelLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 17.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig

class ChannelLoader:
	"""
	"""

	@staticmethod
	def load_channels(chan_names=None, logger=None):
		"""
		:param chan_names: list of strings
		:param logger: logger instance (python logging module)
		:returns: list of ampel.pipeline.config.AmpelConfig instances
		:raises ValueError: if
		  - duplicate values are found in chan_names
		  - channels with duplicate names are found (chan_names == None)
		:raises NameError: if a channel name (chan_names) is not found
		"""

		# list of chan docs
		ret = []

		# Robustness check
		if chan_names is None:
			
			# if chan_names is None, load all available channels (unless non-active)
			for chan_doc in AmpelConfig.get_config('channels'):

				if chan_doc['active'] is False:
					# Do not load channels with active=False if not specifically required (by name) 
					if logger:
						logger.info("Ignoring non-active channel %s" % chan_doc['channel'])
					continue

				ret.append(
					ChannelConfig.parse_obj(chan_doc)
				)

				if len(chan_doc) != len({el['channel'] for el in ret}):
					raise ValueError("Channels with duplicate names")

		else:

			if type(chan_names) is str:
				chan_names = [chan_names]

			if len(chan_names) != len(set(chan_names)):
				raise ValueError("Duplicates found in provided list of channel names")

			# Loop through all channels
			for chan_doc in AmpelConfig.get_config('channels'):

				if chan_doc['channel'] not in chan_names:
					continue

				if chan_doc['active'] is False and logger:
					logger.info("Loading requested non-active channel %s" % chan_doc['channel'])

				ret.append(
					ChannelConfig.parse_obj(chan_doc)
				)

			if len(ret) != len(chan_names):
				for chan_name in chan_names:
					if next(
						filter(
							lambda x: x['channel'] == chan_name, 
							AmpelConfig.get_config('channels')
						), None
					) is None:
						raise NameError("Channel '%s' not found" % chan_name)

		return ret
