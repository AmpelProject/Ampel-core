#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/channel/ChannelConfigLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 19.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.channel.T2UnitConfig import T2UnitConfig
from ampel.config.channel.ChannelConfig import ChannelConfig

class ChannelConfigLoader:
	"""
	Convenience class that creates :obj:`ChannelConfig <ampel.pipeline.config.channel.ChannelConfig>`
	using channel dicts loaded from AmpelConfig (retrieved by channel names).
	"""

	@staticmethod
	def load_configurations(chan_names=None, tier="all", logger=None):
		"""
		Convenience method that creates :obj:`ChannelConfig <ampel.pipeline.config.channel.ChannelConfig>`
		using channel dicts loaded from AmpelConfig (retrieved by channel names).
		If no channel name is provided, all _active_ channels are loaded.
		If a list of name is provided, a warning will be printer (provided argument _logger_ is not None)
		when loaded non-active channels.
		Loading lighter ChannelConfig instances taylored for usage at given tier level is possible 
		using the _tier_ argument (see _tier_ parameter for more info)

		:param chan_names: list of channel names
		:type chan_names: list(str), str
		:param logger: logger instance (python logging module)
		:param tier: at which tier level the returned ChannelConfig will be used. 
		Possible values are: 'all', 0, 3.
		Less checks are performed when restricting tier to 0 or 3 which yields 
		a lighter and quicker ChannelConfig loading procedure. For example, with tier=0, 
		T3 units existence or T3 run configurations are not checked.
		:type tier: str, int

		:raises NameError: if a channel is not found
		:raises ValueError: if duplicate values are found in chan_names
		:returns: list of ampel.pipeline.config.channel.ChannelConfig instances
		:rtype: list(ampel.pipeline.config.channel.ChannelConfig)
		"""

		# list of chan configs
		ret = []

		# Robustness check
		if chan_names is None:
			
			# if chan_names is None, load all available channels (unless non-active)
			for chan_doc in AmpelConfig.get_config('channels').values():

				# None is not False and active defaults to True
				if chan_doc.get('active') is False:
					# Do not load channels with active=False if not specifically required (by name) 
					if logger:
						logger.info("Ignoring non-active channel %s" % chan_doc['channel'])
					continue

				ret.append(
					ChannelConfig.create(tier, **chan_doc)
				)

		else:

			if isinstance(chan_names, (int, str)):
				chan_names = [chan_names]

			if len(chan_names) != len(set(chan_names)):
				raise ValueError("Duplicates found in provided list of channel names")

			# Loop through all channels
			for chan_name in chan_names: 
				
				chan_doc = AmpelConfig.get_config(['channels', chan_name])
				if chan_doc is None:
					raise ValueError("Channel {} is not defined".format(chan_name))

				# None is not False and active defaults to True
				if chan_doc.get('active') is False and logger:
					logger.info("Loading requested non-active channel %s" % chan_doc['channel'])

				ret.append(
					ChannelConfig.create(tier, **chan_doc)
				)

		return ret
