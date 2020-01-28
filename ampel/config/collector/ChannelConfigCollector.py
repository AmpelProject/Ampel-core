#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/collector/ChannelConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 05.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.db.DBUtils import DBUtils
from ampel.config.collector.ConfigCollector import ConfigCollector


class ChannelConfigCollector(ConfigCollector):
	"""
	"""

	def add(self, arg: Dict[str, Any], dist_name: str = None) -> None:
		""" """

		if "channel" not in arg:
			self.missing_key("channel", "channel", dist_name)
			return

		try:

			chan_name = arg.get('channel')

			if self.verbose:
				self.logger.verbose("-> Adding channel: " + chan_name)

			if 'distName' in arg:
				dist_name = arg['distName']
			else:
				if dist_name:
					arg['distName'] = dist_name

			# Check duplicated channel names
			if self.get(chan_name):
				self.duplicated_entry(chan_name, new_dist=dist_name)
				return

			if "NO_HASH" not in arg.get('policy', []):
				arg['hash'] = DBUtils.b2_hash(chan_name)
				for k, v in self.items():
					if arg['hash'] == v.get('hash'):
						raise ValueError(
							f"Channel name hash collision detected. "
							f"Channel name 1: {k}. Hash value: {v.get('hash')}"
							f"Channel name 2: {arg['channel']}. Hash value: {arg.get('hash')}"
						)

			self.__setitem__(chan_name, arg)

		except Exception as e:
			self.error(
				f"Error occured while loading channel config {self.distrib_hint(dist_name)}. "
				f"Offending value: {arg}",
				exc_info=e
			)
