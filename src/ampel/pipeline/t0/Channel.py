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
from ampel.pipeline.logging.T0RejConsoleFormatter import T0RejConsoleFormatter

class Channel:
	"""
	This class instantiates loggers and elements such as T0 filter classes
	(using ampel.pipeline.config.channel.ChannelConfig)

	A note regarding logging: 
	AmpelLogger can be setup with a default 'extra' parameter (with a channel for example),
	which is convenient when passing a logger to a contrib unit whose data originates for known channels.
	It is used at the T3 level for example.

	At the T0 level however, the majority of alerts do not pass the filters and the produced log entries
	go into a dedicated db collection named after the channel name. 
	Therefore, a 'channels' property does not need to be added to the log entries.

	We could add the 'channels' property to the logger anyway and remove it later in DBRejectedLogsSaver, 
	but better is not to add it at all. 

	So we we optimize logging for the most frequent case by doing the following:

	1) For accepeted alerts:
	For the few times where we accept an alert, we add 'channels' to the parameter 'extra' of
	log entries before passing them to the DBLoggingHandler. That's what the option 
	'channels' in RecordsBufferingHandler.copy() or RecordsBufferingHandler.forward() is for.

	2) For rejected alerts:
	Nothing should be done except (possibly) when full_console_logging=True in the AlertProcessor 
	(which is not the case in production): no channel info appears in the console logs 
	since channel info is not included by default. 
	To make the console output consistent between rejected and accepted alerts, we can artificially
	add the channel info to log entries outputed to the console. It is done in T0RejConsoleFormatter 
	(variable 'implicit_channels'). Again, on production, the T0RejConsoleFormatter of each Channel 
	(this class) is not used because full_console_logging is False.

	Second Note: T0 filter units get a dedicated logger associated with a RecordBufferingHandler because 
	we route the produced logs either to the standard logger or to the "rejected logger" depending
	whether the alert is accepeted or not.
	"""

	def __init__(self, chan_config, survey_id, parent_logger, log_line_nbr=False, embed=False, single_rej_col=False):
		"""
		:param ChannelConfig chan_config: instance of :obj:`ChannelConfig \
			<ampel.pipeline.config.channel.ChannelConfig>`
		:param str survey_id: name of the survey id (ex:ZTFIPAC)
		:param Logger parent_logger: logger instance (python module logging)
		:param bool embed: 
		:param bool single_rej_col: 
			- False: rejected logs are saved in channel specific collections
			 (collection name equals channel name)
			- True: rejected logs are saved in a single collection called 'logs'
		:raises: NameError if the provided survey id is not defined as source in the channel config 
		:returns: None
		"""

		self.stream_config = chan_config.get_stream_config(survey_id)
		if self.stream_config is None:
			raise NameError("Unknown survey id: '%s'" % survey_id)

		# Channel name (ex: HU_SN, 1)
		self.name = chan_config.channel
		self.str_name = str(self.name) if type(self.name) is int else self.name

		self.auto_complete = self.stream_config.parameters.get('autoComplete', False)
		self.unit_name = self.stream_config.t0Filter.unitId

		# Instantiate/get filter class associated with this channel
		parent_logger.info("Loading filter: %s" % self.unit_name)

		# Raise exception if not found / invalid
		FilterClass = AmpelUnitLoader.get_class(
			tier=0, unit_name=self.unit_name, raise_exc=True
		)

		self.t2_units = {el.unitId for el in self.stream_config.t2Compute}
		parent_logger.info("On match t2 units: %s" % self.t2_units)

		# Create channel (buffering) logger
		self.buff_logger = AmpelLogger("buff_" + self.str_name)
		self.buff_handler = RecordsBufferingHandler(embed)
		self.buff_logger.addHandler(self.buff_handler)

		self.filter_func = FilterClass(
			self.t2_units,
			base_config = AmpelUnitLoader.get_resources(FilterClass),
			run_config = self.stream_config.t0Filter.runConfig, 
			logger = self.buff_logger
		).apply

		# Clear possibly existing log entries (logged by FilterClass__init__) to parent_logger
		self.buff_handler.buffer = []

		self.rejected_logger = AmpelLogger.get_logger(
			name=self.str_name + "_rej", 
			formatter=T0RejConsoleFormatter(
				line_number=log_line_nbr,
				implicit_channels=self.name
			),
			log_level=logging.WARNING
		)

		self.rejected_log_handler = DBRejectedLogsSaver(
			self.str_name, parent_logger, single_rej_col
		)
		self.rejected_logger.addHandler(self.rejected_log_handler)
