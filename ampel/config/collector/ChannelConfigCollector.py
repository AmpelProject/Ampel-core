#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/ChannelConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  02.01.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE
from ampel.util.hash import hash_payload


class ChannelConfigCollector(AbsDictConfigCollector):

	def add(self,
		chan_dict: dict[str, Any],
		dist_name: str,
		version: str | float | int,
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

			# Check duplicated channel names between distribs
			if self.check_duplicates(chan_name, dist_name, version, register_file):
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
