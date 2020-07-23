#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ChannelConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from ampel.util.crypto import b2_short_hash
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class ChannelConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:

		if 'channel' not in arg:
			self.missing_key('channel', 'channel', file_name, dist_name)
			return

		try:

			chan_name = arg['channel']

			if self.verbose:
				self.logger.log(VERBOSE, f'Adding channel: {chan_name}')

			if 'distrib' in arg:
				dist_name = arg['distrib']
			else:
				if dist_name:
					arg['distrib'] = dist_name

			if 'source' in arg:
				file_name = arg['source']
			else:
				if file_name:
					arg['source'] = file_name

			# Check duplicated channel names
			if self.get(chan_name):
				self.duplicated_entry(
					conf_key = chan_name,
					new_file = file_name,
					new_dist = dist_name,
					prev_file = self.get(chan_name).get('conf', 'unknown'), # type: ignore
					prev_dist = self.get(chan_name).get('distrib', 'unknown') # type: ignore
				)
				return

			if not ('NO_HASH' in arg.get('policy', []) or isinstance(chan_name, int)):
				arg['hash'] = b2_short_hash(chan_name)
				for k, v in self.items():
					if arg['hash'] == v.get('hash'):
						raise ValueError(
							f'Channel name hash collision detected. '
							f'Channel name 1: {k}. Hash value: {v.get("hash")}'
							f'Channel name 2: {arg["channel"]}. Hash value: {arg.get("hash")}'
						)

			self.__setitem__(chan_name, arg)

		except Exception as e:
			self.error(
				f'Error occured while loading channel config {self.distrib_hint(file_name, dist_name)}. '
				f'Offending value: {arg}',
				exc_info=e
			)
