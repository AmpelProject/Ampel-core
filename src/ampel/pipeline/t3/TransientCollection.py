#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientCollection.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.02.2018
# Last Modified Date: 22.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.ChannelFlags import ChannelFlags
from ampel.flags.FlagUtils import FlagUtils
import logging


class TransientCollection:
	"""
	"""

	def __init__(self, db, logger=None, collection="main"):

		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.col = db[collection]


	def get_ids(self):
		pass


	def has_transient_with_errors(self):
		pass


	def get_transients_with_flags(self, flags=None):
		pass


	def get_transients_iterator(self):
		pass


	def get_transient(self, tran_id):
		pass
