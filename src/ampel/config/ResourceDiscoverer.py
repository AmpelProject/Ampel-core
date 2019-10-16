#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/ResourceDiscoverer.py
# License           : BSD-3-Clause
# Author            : jvs, vb
# Date              : 17.09.2018
# Last Modified Date: 17.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib, pkg_resources
from ampel.config.channel.ChannelLoader import ChannelLoader
from ampel.t2.T2Controller import T2Controller

class ResourceDiscoverer:
	"""
	"""

	@staticmethod
	def get_t0_resources(source, chan_names=None):
		""" 
		:param source: str. Source of the alert stream (ex: ZTFIPAC)
		:param chan_names: optional. Specifically load channels with provided names (list of strings)
		:returns: list of ressource names (str)
		"""
		resources = set()
		for chan_config in ChannelLoader.load_channels(chan_names=chan_names):
			stream_config = chan_config.get_stream_config(source)
			filter_entry_point = next(
				pkg_resources.iter_entry_points('ampel.t0', stream_config.t0Filter.unitId)
			)
			module = importlib.import_module(filter_entry_point.module_name)
			filter_class = getattr(module, stream_config.t0Filter.unitId)
			resources.update(filter_class.resources)
		return resources


	@staticmethod
	def get_t2_resources():
		""" 
		:returns: list of ressource names (str)
		"""
		resources = set()
		for ep in pkg_resources.iter_entry_points('ampel_t2Units'):
			t2_class = T2Controller.load_unit(ep.name)
			resources.update(t2_class.resources)
		return resources
