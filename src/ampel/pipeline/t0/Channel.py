#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/Channel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 14.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources, logging
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.logging.RecordsBufferingHandler import RecordsBufferingHandler
from ampel.pipeline.logging.DBRejectedLogsSaver import DBRejectedLogsSaver

class Channel:
	"""
	Wrapper around an instance of ampel.pipeline.config.channel.ChannelConfig
	that contains elements such as an instantiated filter function 
	(as defined in the channel parameter)
	"""

	def __init__(self, chan_config, survey_id, parent_logger):
		"""
		:param ChannelConfig chan_config: instance of :obj:`ChannelConfig \
			<ampel.pipeline.config.channel.ChannelConfig>`
		:param str survey_id: name of the survey id (ex:ZTFIPAC)
		:param Logger parent_logger: logger instance (python module logging)
		:raises: NameError if the provided survey id is not defined as source in the channel config 
		:returns: None
		"""

		self.stream_config = chan_config.get_stream_config(survey_id)
		if self.stream_config is None:
			raise NameError("Unknown survey id: '%s'" % survey_id)

		# Channel name (ex: HU_SN)
		self.name = chan_config.channel

		self.auto_complete = self.stream_config.parameters.get('autoComplete', False)

		# Instantiate/get filter class associated with this channel
		parent_logger.info("Loading filter: %s" % self.stream_config.t0Filter.unitId)

		# Raise exception if not found / invalid
		FilterClass = AmpelUnitLoader.get_class(
			tier=0, unit_name=self.stream_config.t0Filter.unitId, raise_exc=True
		)

		self.t2_units = {el.unitId for el in self.stream_config.t2Compute}
		# Feedback
		parent_logger.info("On match t2 units: %s" % self.t2_units)

		# Create channel (buffering) logger
		#self.buff_logger = AmpelLogger(self.name, channels=self.name)
		self.buff_logger = AmpelLogger(self.name)

		# Shortcut used by AlertProcessor
		self.log_extra = self.buff_logger._AmpelLogger__extra

		sh = next(filter( # Will raise Exception if not found
			lambda hdlr: isinstance(hdlr, logging.StreamHandler), 
			parent_logger.handlers
		))

		self.buff_logger.addHandler(sh)
		self.buff_handler = RecordsBufferingHandler(self.name)
		self.buff_logger.addHandler(self.buff_handler)

		self.filter_func = FilterClass(
			self.t2_units,
			base_config = AmpelUnitLoader.get_resources(FilterClass),
			run_config = self.stream_config.t0Filter.runConfig, 
			logger = self.buff_logger
		).apply

		self.rejected_logs_saver = DBRejectedLogsSaver(self.name, parent_logger)


	def set_log_extra(self, log_extra):
		""" """
		self.buff_logger._AmpelLogger__extra = log_extra
