#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/APFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.model.t0.APChanData import APChanData
from ampel.db.AmpelDB import AmpelDB
from ampel.core.AmpelUnitLoader import AmpelUnitLoader
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.DBRejectedLogsSaver import DBRejectedLogsSaver
from ampel.logging.T0RejConsoleFormatter import T0RejConsoleFormatter
from ampel.logging.RecordsBufferingHandler import RecordsBufferingHandler

class APFilter:
	"""
	Helper class for the AlertProcessor.
	It instantiates and references loggers, T0 filter and the list of T2 tickets to be created
	A better class name is welcome.

	A note regarding logging: 
	AmpelLogger can be setup with a default 'extra' parameter (with a channel for example),
	which is convenient when passing a logger to a contrib unit whose data originates for known channels.
	It is used at the T3 level for example.

	At the T0 level however, the majority of alerts do not pass the filters and the produced log entries
	go into a dedicated db collection named after the channel name. 
	Therefore, a 'channels' property does not need to be added to the log entries.

	We could add the 'channels' property to the logger anyway and remove it later in DBRejectedLogsSaver, 
	but better is not to add it at all. 

	So we optimize logging for the most frequent case by doing the following:

	1) For accepted alerts:
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

	def __init__(
		self, ampel_db: AmpelDB, ampel_unit_loader: AmpelUnitLoader, ap_chan_data: APChanData,
		parent_logger: AmpelLogger, log_line_nbr=False, embed=False, single_rej_col=False
	):
		"""
		:param bool single_rej_col: 
		- False: rejected logs are saved in channel specific collections
		 (collection name equals channel name)
		- True: rejected logs are saved in a single collection called 'logs'
		"""

		# Channel name (ex: HU_SN, 1)
		self.channel = ap_chan_data.name
		self.chan_str = str(self.channel) if isinstance(self.channel, int) else self.channel
		self.auto_complete = ap_chan_data.auto_complete

		# Create channel (buffering) logger
		self.logger = AmpelLogger("buf_" + self.chan_str)
		self.rec_buf_hdlr = RecordsBufferingHandler(embed)
		self.logger.addHandler(self.rec_buf_hdlr)

		# Instantiate/get filter class associated with this channel
		parent_logger.info(
			f"Loading filter: {ap_chan_data.t0_add.unit.class_name}"
		)

		ampel_unit = ampel_unit_loader.get_ampel_unit(
			ap_chan_data.t0_add.unit
		)

		self.t2_units = {
			el.class_name for el in ap_chan_data.t0_add.t2_compute
		}

		ampel_unit.init_config.on_match_t2_units = self.t2_units
			
		self.filter_func = ampel_unit \
			.instantiate(self.logger) \
			.apply

		parent_logger.info(
			f"On match t2 units: {self.t2_units}"
		)

		# Clear possibly existing log entries 
		# (logged by FilterClass__init__) to parent_logger
		self.rec_buf_hdlr.buffer = []

		self.rejected_logger = AmpelLogger.get_logger(
			name = self.chan_str + "_rej", 
			formatter = T0RejConsoleFormatter(
				line_number = log_line_nbr,
				implicit_channels = self.channel
			)
		)

		self.rejected_log_handler = DBRejectedLogsSaver(
			ampel_db, self.chan_str, parent_logger, single_rej_col
		)

		self.rejected_logger.addHandler(
			self.rejected_log_handler
		)
