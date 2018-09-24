#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/T0Channel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 17.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources, importlib
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.AmpelConfig import AmpelConfig

class T0Channel:
	"""
	Wrapper around an instance of ampel.pipeline.config.channel.ChannelConfig
	that contains elements such as an instantiated filter function 
	(as defined in the channel parameter)
	"""

	def __init__(self, chan_config, survey_id, logger):
		"""
		:param chan_config: instance of ampel.pipeline.config.channel.ChannelConfig
		:param str survey_id: name of the survey id (ex:ZTFIPAC)
		:param logger: logger instance (python module logging)
		:returns: None
		:raises: NameError if:
			- the provided survey id is not defined as source in the channel config 
			- filter id (stream_config.t0Filter.unitId) is unknown 
			  (not defined in the pkg_resources entry points)
			- a t2 unit id (stream_config.t2Compute.unitId) is unknown 
			  (not defined in the pkg_resources entry points)
		"""

		self.stream_config = chan_config.get_stream_config(survey_id)
		if self.stream_config is None:
			raise NameError("Unknown survey id: '%s'" % survey_id)

		# Channel name (ex: HU_SN)
		self.name = chan_config.channel

		# Build these two log entries once and for all
		self.log_accepted = " -> %s: alert passes filter criteria" % chan_config.channel
		self.log_rejected = " -> %s: alert was rejected" % chan_config.channel

		self.auto_complete = False
		if self.stream_config.parameters['autoComplete']:
			self.auto_complete = True
			self.log_auto_complete = " -> %s: accepting alert (auto-complete)" % chan_config.channel

		# Get t0 filter id
		filter_id = self.stream_config.t0Filter.unitId
		filter_entry_point = next(
			pkg_resources.iter_entry_points('ampel.pipeline.t0', filter_id), 
			None
		)

		if filter_entry_point is None:
			raise NameError("Filter '%s' not found" % filter_id)

		logger.info("Loading filter: %s" % filter_entry_point.module_name)

		# Instantiate filter class associated with this channel
		module = importlib.import_module(filter_entry_point.module_name)
		Filter_class = getattr(module, filter_id)
		base_config = {}

		if hasattr(Filter_class, 'resources'):
			for k in Filter_class.resources:
				base_config[k] = AmpelConfig.get_config('resources.{}'.format(k))

		self.t2_units = {el.unitId for el in self.stream_config.t2Compute}
		known_t2s = {el.name for el in pkg_resources.iter_entry_points('ampel.pipeline.t2.units')}

		if len(self.t2_units - known_t2s) > 0:
			raise NameError(
				"Unknown T2 unit(s) '%s' referenced by channel '%s'" % (
					str(list(self.t2_units - known_t2s)), chan_config.channel
				)
			)

		logger.info("On match t2 units: %s" % self.t2_units)

		self.filter_func = Filter_class(
			self.t2_units,
			base_config = base_config,
			run_config = self.stream_config.t0Filter.runConfig, 
			logger=logger
		).apply

		self.resources = Filter_class.resources
