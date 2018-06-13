#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/ZICompoundShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.CompoundFlags import CompoundFlags
from ampel.abstract.AbsCompoundShaper import AbsCompoundShaper

flag_pos = {}
for flag_name in ("ZTF_COLLAB", "SRC_AMPEL", "SUPERSEEDED", "HAS_HUMBOLDT_ZP", "HAS_WEIZMANN_SUB"):
	flag_pos[flag_name] = FlagUtils.get_flag_pos_in_enumflag(
		PhotoFlags[flag_name]
	)

class ZICompoundShaper(AbsCompoundShaper):
	"""
	"""

	version = 1.0
	default_flag = CompoundFlags.INST_ZTF|CompoundFlags.SRC_IPAC


	def __init__(self, chan_opts):
		""" """
		self.flags = ZICompoundShaper.default_flag
		self.chan_opts = chan_opts


	def gen_compound_item(self, input_d, channel_name):
		"""	
		"""	

		# Photopoint ids are referenced by the key name 'pp' 
		# whereas upper limis ids are referenced by the key name 'ul'
		id_key = 'pp' if 'magpsf' in input_d.keys() else 'ul'
		comp_entry = {id_key: input_d['_id']}
		opts = self.chan_opts[channel_name]

		############
		# POLICIES #
		############

		#  Photopoint option: check if updated zero point should be used
		if opts['updatedHUZP'] is True and flag_pos["HAS_HUMBOLDT_ZP"] in input_d['alFlags']:
			comp_entry['huZP'] = 1
			self.flags |= CompoundFlags.WITH_CUSTOM_POLICES

		#  Photopoint option: check if alternative subtraction method should be used
		# if opts['weizmannSub'] is True and flag_pos["HAS_WEIZMANN_SUB"] in input_d['alFlags']:
		#	comp_entry['wzm'] = 1


		##############
		# EXCLUSIONS #
		##############

		# Check access permission (public / partners)
		if flag_pos["ZTF_COLLAB"] in input_d['alFlags']:
			self.flags |= CompoundFlags.ZTF_COLLAB_DATA
			if opts['ZTFPartner'] is False:
				comp_entry['excl'] = "Private"
				self.flags |= CompoundFlags.HAS_DATARIGHTS_EXCLUSION

		# Check autocomplete
		elif opts['autoComplete'] is False and flag_pos["SRC_AMPEL"] in input_d['alFlags']:
			comp_entry['excl'] = "Autocomplete"
			self.flags |= CompoundFlags.HAS_EXCLUDED_PPS|CompoundFlags.HAS_AUTOCOMPLETED_PHOTO

		# Channel specific photophoint exclusion. input_d["alExcl"] could look like this: 
		# ["HU_SN", "HU_GRB"]
		elif "alExcl" in input_d and channel_name in input_d["alExcl"]:
			comp_entry['excl'] = "Manual"
			self.flags |= CompoundFlags.HAS_EXCLUDED_PPS|CompoundFlags.HAS_MANUAL_EXCLUSION

		#  Check for superseeded 
		elif flag_pos["SUPERSEEDED"] in input_d['alFlags']:
			comp_entry['excl'] = "Superseeded"
			self.flags |= CompoundFlags.HAS_EXCLUDED_PPS|CompoundFlags.HAS_SUPERSEEDED_PPS

		return comp_entry
