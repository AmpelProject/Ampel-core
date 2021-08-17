#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ChannelConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 19.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union
from ampel.util.hash import hash_payload
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class ChannelConfigCollector(AbsDictConfigCollector):

	def add(self,
		chan_dict: Dict[str, Any],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str
	) -> None:

		if 'channel' not in chan_dict:
			self.missing_key(
				what='channel', key='channel',
				dist_name=dist_name, register_file=register_file
			)
			return

		try:

			chan_name = chan_dict['channel']

			if self.verbose:
				self.logger.log(VERBOSE, f'Adding channel: {chan_name}')

			if 'distrib' not in chan_dict:
				chan_dict['distrib'] = dist_name

			if 'source' not in chan_dict:
				chan_dict['source'] = register_file

			if 'version' not in chan_dict:
				chan_dict['version'] = version

			# Check duplicated channel names
			if self.get(chan_name):
				self.duplicated_entry(
					conf_key = chan_name,
					new_file = chan_dict['source'],
					new_dist = chan_dict['distrib'],
					prev_file = self.get(chan_name).get('conf', 'unknown'), # type: ignore
					prev_dist = self.get(chan_name).get('distrib', 'unknown') # type: ignore
				)
				return

			if not ('NO_HASH' in chan_dict.get('policy', []) or isinstance(chan_name, int)):
				chan_dict['hash'] = hash_payload(chan_name)
				for k, v in self.items():
					if chan_dict['hash'] == v.get('hash'):
						raise ValueError(
							f'Channel name hash collision detected. '
							f'Channel name 1: {k}. Hash value: {v.get("hash")}'
							f'Channel name 2: {chan_dict["channel"]}. Hash value: {chan_dict.get("hash")}'
						)

			self.__setitem__(chan_name, chan_dict)

		except Exception as e:
			self.error(
				f'Error occured while loading channel config {self.distrib_hint(dist_name, register_file)}. '
				f'Offending value: {chan_dict}',
				exc_info=e
			)
