#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ChannelConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union
from ampel.util.crypto import b2_short_hash
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class ChannelConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str
	) -> None:

		if 'channel' not in arg:
			self.missing_key(what='channel', key='channel', dist_name=dist_name, register_file=register_file)
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
				register_file = arg['source']
			else:
				if register_file:
					arg['source'] = register_file

			# Check duplicated channel names
			if self.get(chan_name):
				self.duplicated_entry(
					conf_key = chan_name,
					new_file = register_file,
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
				f'Error occured while loading channel config {self.distrib_hint(dist_name, register_file)}. '
				f'Offending value: {arg}',
				exc_info=e
			)
