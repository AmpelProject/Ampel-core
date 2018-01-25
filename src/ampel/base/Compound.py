#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/CompoundGenerator.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 09.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, hashlib, json
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.ChannelFlags import ChannelFlags


class Compound():
	"""
		Documentation will follow
	"""

	def __init__(self, dbdoc):

		if dbdoc["alDocType"] != AlDocTypes.COMPOUND:
			raise ValueError("The provided document is not a compound")

		self.pp_ids = set()
		self.ppsopt = dict()

		for el in dbdoc['pps']:
			self.pp_ids.add(el['pp'])
			if (len(el.keys()) > 1):
				self.ppsopt[el['pp']] = el


