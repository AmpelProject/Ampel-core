#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/ZICompElement.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2018
# Last Modified Date: 09.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, hashlib, json
from ampel.abstract.AbsCompElement import AbsCompElement
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

flag_pos = {}
for flag_name in ("ZTF_COLLAB", "SRC_AMPEL", "SUPERSEEDED", "HAS_HUMBOLDT_ZP", "HAS_WEIZMANN_SUB"):
	flag_pos[flag_name] = FlagUtils.get_flag_pos_in_enumflag(
		PhotoFlags[flag_name]
	)

class ZICompElement(AbsCompElement):
	"""
	This class requires documentation to be understood.
	I'll try to do that in the near future (have it on paper)
	Name shortcuts used in this class:
	"eid": effective id
	"sid": strict id
	"chan": channel name
	"flagpos": index position of flag
	"comp": compound
	"opt": option
	"sig": signature
	"""

	version = 1.0


	def __init__(self, channels):
		"""
		channel: instance of ampel.pipeline.config.Channel
		(For T0: each 'active' channel must be added) 
		"""
		# Input parameters example:
		# { "ZTFPartner" : true, "autoComplete" : true, "updatedHUZP" : false }
		self.chan_opts = {chan.name: chan.get_config("parameters") for chan in channels}


	def gen_dict(self, input_d, channel_name):
		"""	
		"""	

		# Get chan option
		chan_opts = self.chan_opts[channel_name]
	
		# Photopoint ids are referenced by the key name 'pp' 
		# whereas upper limis ids are referenced by the key name 'ul'
		id_key = 'pp' if 'magpsf' in input_d else 'ul'
		comp_entry = {id_key: input_d['_id']}


		############
		# POLICIES #
		############

		#  Photopoint option: check if updated zero point should be used
		if chan_opts['updatedHUZP'] is True and flag_pos["HAS_HUMBOLDT_ZP"] in input_d['alFlags']:
			comp_entry['huZP'] = 1

		#  Photopoint option: check if alternative subtraction method should be used
		# if chan_opts['weizmannSub'] is True and flag_pos["HAS_WEIZMANN_SUB"] in input_d['alFlags']:
		#	comp_entry['wzm'] = 1


		##############
		# EXCLUSIONS #
		##############

		# Check access permission (public / partners)
		if chan_opts['ZTFPartner'] is False and flag_pos["ZTF_COLLAB"] in input_d['alFlags']:
			comp_entry['excl'] = "Private"

		# Check autocomplete
		elif chan_opts['autoComplete'] is False and flag_pos["SRC_AMPEL"] in input_d['alFlags']:
			comp_entry['excl'] = "Autocomplete"

		# Channel specific photophoint exclusion. input_d["alExcl"] could look like this: 
		# ["HU_SN", "HU_GRB"]
		elif "alExcl" in input_d and channel_name in input_d["alExcl"]:
			comp_entry['excl'] = "Manual"

		#  Check for superseeded 
		elif flag_pos["SUPERSEEDED"] in input_d['alFlags']:
			comp_entry['excl'] = "Superseeded"

		return comp_entry
