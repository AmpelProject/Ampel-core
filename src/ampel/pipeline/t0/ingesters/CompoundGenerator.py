#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/CompoundGenerator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, hashlib, json
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

HAS_HUMBOLDT_ZP = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.HAS_HUMBOLDT_ZP)
HAS_WEIZMANN_SUB = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.HAS_WEIZMANN_SUB)
SUPERSEEDED = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SUPERSEEDED)
ZTF_COLLAB = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.ZTF_COLLAB)
SRC_AMPEL = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SRC_AMPEL)


class CompoundGenerator():
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

	channel_options = {}
	chan_opts_sig = {} # channel options signature
	flagpos_to_check = {SUPERSEEDED}


	@classmethod
	def cm_init_channels(cls, channels):
		"""
		Sets required internal static variables.
		channels: list of instances of ampel.pipeline.config.Channel
		For T0: channels must contain the active channels.
		"""
		for channel in channels:
			cls.cm_init_channel(channel)


	@classmethod
	def cm_init_channel(cls, channel):
		"""
		Set required internal static variables.
		channel: instance of ampel.pipeline.config.Channel
		(For T0: each 'active' channel must be added) 
		"""

		chan_name = channel.get_name()

		# In case this method is called multiple times
		if chan_name in cls.channel_options:
			del cls.channel_options[chan_name]

		# Input parameters example:
		# { 
		#	"ZTFPartner" : true, 
		#	"autoComplete" : true, 
		#	"updatedHUZP" : false 
		# }
		cls.channel_options[chan_name] = channel.get_input().get_parameters()

		# shortcut
		chan_opts = cls.channel_options[chan_name]

		# Generate options signature (example: "10011")
		opts_sig = ""
		for key in sorted(chan_opts.keys()):
			opts_sig += "1" if chan_opts[key] else "0"

		# Save option signature as static variable
		cls.chan_opts_sig[chan_name] = opts_sig

		# flagpos_to_check is used for optimization of the init routine
		if chan_opts['ZTFPartner'] is False:
			cls.flagpos_to_check.add(ZTF_COLLAB)

		if chan_opts['autoComplete'] is False:
			cls.flagpos_to_check.add(SRC_AMPEL)

		if "updatedHUZP" in chan_opts and chan_opts['updatedHUZP'] is True:
			cls.flagpos_to_check.add(HAS_HUMBOLDT_ZP)

		if "weizmannSub" in chan_opts and chan_opts['weizmannSub'] is True:
			cls.flagpos_to_check.add(HAS_WEIZMANN_SUB)


	def __init__(self, pps_db, ids_alert, logger):
	#def __init__(self, pps_db, pp_ids_alert, uls_db, ul_ids_alert, logger):
		"""
		pps_db: list of photopoints dict instances loaded from db
		ids_alert: set of candid (longs)
		"""

		self.logger = logger

		# Create set for DB photopoint ids (list of db pps provided as func parameter)
		self.ids_db = set()

		# Channel based exclusions are a feature of Ampel and thus independant from channel configurations. 
		# Dict key is channel name, dict value is the associated set of photopoint ids
		self.d_ids_excluded = {}

		# Dict with flag index pos (integer) as key (such as SRC_T1, ZTF_COLLAB, ...)
		# and the corresponding set of ids as value
		self.d_ids_sets = {}

		# We need a set for the ids of superseeded pps
		self.d_ids_sets[SUPERSEEDED] = set()

		# flagpos_to_check was set during static init depending on the configuration of 
		# each added channel. We do not need {sets of id} for every existing option but only 
		# for the flags defined in flagpos_to_check since it can happen that no channel 
		# uses the option say 'updatedHUZP'
		for flag in CompoundGenerator.flagpos_to_check:
			self.d_ids_sets[flag] = set()

		# Loop through provided list of pps in order to populate the different set of ids
		for pp in pps_db:

			# Build set of DB ids
			self.ids_db.add(pp["_id"])

			# Check photopoint flags defined in flagpos_to_check and populate associated sets
			for flag_pos in CompoundGenerator.flagpos_to_check:
				if flag_pos in pp['alFlags']:
					self.d_ids_sets[flag_pos].add(pp['_id'])

			# Channel specific photophoint exclusion. pp["alExcluded"] could look like this: 
			# {"CHANNEL_SN", "CHANNEL_GRB"}
			if "alExcluded" in pp:
				for chan_str in pp["alExcluded"]:
					if not chan_str in self.d_ids_excluded.keys():
						self.d_ids_excluded[chan_str] = {pp['_id']} 
					else:
						self.d_ids_excluded[chan_str].add(pp['_id'])
			
		# Sort photopoints by id (type: long)
		self.pp_ids = sorted(self.ids_db.union(ids_alert))


		# 1
		self.d_eid_comp = {}

		# 2
		self.d_eid_chanset = {}

		# 3
		self.d_eid_tuple_chan_sid = {}

		# 4
		self.d_sid_compdiff = {}

		# 5
		self.d_optsig_eid = {}

		# 6
		self.d_optsig_sid = {}

			
	def get_db_ids(self):
		""" 
		Returns a python set containing the "_id" of every photopoint contained in the DB
		"""
		return self.ids_db


	def add_newly_superseeded_id(self, superseeded_id):
		self.d_ids_sets[SUPERSEEDED].add(superseeded_id)


	def get_eff_compound(self, compound_id):
		return self.d_eid_comp[compound_id]


	def get_t2_flavors(self, compound_id):
		"""	
		"""	
		return [
			{
				'channel': [ell.name for ell in el[0].as_list()],
				'flavor': el[1]
			}
			for el in self.d_eid_tuple_chan_sid[compound_id]
		]


	def has_flavors(self, compound_id):
		"""	
		"""	
		if not compound_id in self.d_eid_tuple_chan_sid:
			return False

		if len(self.d_eid_tuple_chan_sid[compound_id]) == 0:
			return False

		return True


	def get_compound_flavors(self, compound_id):
		"""
		"""
		return [
			{'flavor': el[1], 'omitted': self.d_sid_compdiff[el[1]]}
			for el in self.d_eid_tuple_chan_sid[compound_id]
		]

	
	def get_compound_ids(self, chan_set):
		"""
		set of channel names (string)
		"""

		compound_ids = set()

		for chan_str in chan_set:
			options_sig = CompoundGenerator.chan_opts_sig[chan_str]
			compound_ids.add(self.d_optsig_eid[options_sig])

		return compound_ids


	def get_channels_for_compoundid(self, compound_id):
		"""
		"""
		return self.d_eid_chanset[compound_id]


	def generate(self, chan_set):
		"""	
		set of channel names (string)
		"""	

		for chan_str in chan_set:

			#######################################################
			# Check for identical previously generated compound
			# (several channels can have the same config parameters)
			#######################################################
	
			# Get channel option signature (ex: "01101011")
			opts_sig = CompoundGenerator.chan_opts_sig[chan_str]
	
			# if, given the current channel options, effective id was already computed 
			if opts_sig in self.d_optsig_eid:

				# Retrieve previously computed effective id
				eff_id = self.d_optsig_eid[opts_sig]

				# Add current channel name to the set associated with this effective id
				self.d_eid_chanset[eff_id].add(chan_str)
	
				# if a strict id exists for the current channel options
				if opts_sig in self.d_optsig_sid:

					# Retrieve strict id
					strict_id = self.d_optsig_sid[opts_sig]

					# Feedback
					log_output = "(eff: " + eff_id + ", strict: " + strict_id + ")"

					# Add tupple (chan_str, strict id) to internal dict using eff_id as key
					if not eff_id in self.d_eid_tuple_chan_sid:
						self.d_eid_tuple_chan_sid[eff_id] = set()

					self.d_eid_tuple_chan_sid[eff_id].add(
						(chan_str, strict_id)
					)

				else:
					log_output = eff_id 
	
				self.logger.info(
					"Using previoulsy generated compound for channel %s. CompoundId: %s", 
					chan_str, log_output
				)
	
				continue 
	
	
			##########################################
			# Generate compoundId, flavor and compound
			##########################################
	
			eff_comp = []
			strict_comp = []
			eff_hash_payload = ""
			strict_hash_payload = ""
			chan_options = CompoundGenerator.channel_options[chan_str]
	
			# Create compound and compoundId
			for pp_id in self.pp_ids:
	
				d = {'pp': pp_id}
	
				#  Check for exclusions
				if pp_id in self.d_ids_sets[SUPERSEEDED]:
					d['excl'] = SUPERSEEDED
				elif chan_options['ZTFPartner'] is False and pp_id in self.d_ids_sets[ZTF_COLLAB]:
					d['excl'] = ZTF_COLLAB
				elif chan_options['autoComplete'] is False and pp_id in self.d_ids_sets[SRC_AMPEL]:
					d['excl'] = SRC_AMPEL
				elif chan_str in self.d_ids_excluded and pp_id in self.d_ids_excluded[chan_str]:
					d['excl'] = chan_str
	
				#  Photopoint option: check if updated zero point should be used
				if chan_options['updatedHUZP'] is True and pp_id in self.d_ids_sets[HAS_HUMBOLDT_ZP]:
					d['huZP'] = 1
	
				#  Photopoint option: check if alternative subtraction method should be used
				if chan_options['weizmannSub'] is True and pp_id in self.d_ids_sets[HAS_WEIZMANN_SUB]:
					d['wzm'] = 1
	
				eff_comp.append(d)
	
				if len(d) == 1:
					tmp_str = str(pp_id)
					eff_hash_payload += tmp_str
					strict_hash_payload += tmp_str
				else:
					strict_hash_payload += json.dumps(d, sort_keys=True)
					if not 'excl' in d:
						eff_hash_payload += json.dumps(d, sort_keys=True)
					else:
						strict_comp.append(d)
	
			# eff_id = effective compound id = md5 hash of effective compound
			eff_id = hashlib.md5(bytes(eff_hash_payload, "utf-8")).hexdigest()

			# strict_id = strict compound id = md5 hash of strict compound
			strict_id = hashlib.md5(bytes(strict_hash_payload, "utf-8")).hexdigest()
	
			
			################
			# Save results #
			################
	
			# Save channel name to dict using eff_id as key
			if eff_id in self.d_eid_chanset:
				self.d_eid_chanset[eff_id].add(chan_str)
			else:
				self.d_eid_chanset[eff_id] = {chan_str}

			self.d_eid_comp[eff_id] = eff_comp
			self.d_optsig_eid[opts_sig] = eff_id
	
			if eff_id != strict_id:
	
				self.logger.info(
					"Compound generated for channel %s. CompoundId: (eff: %s, strict: %s)", 
					chan_str, eff_id, strict_id
				)
	
				# Save strict id using channel option signature as key
				self.d_optsig_sid[opts_sig] = strict_id

				# Add tupple (chan_str, strict id) to internal dict using eff_id as key
				if not eff_id in self.d_eid_tuple_chan_sid:
					self.d_eid_tuple_chan_sid[eff_id] = set()

				self.d_eid_tuple_chan_sid[eff_id].add(
					(chan_str, strict_id)
				)

				self.d_sid_compdiff[strict_id] = strict_comp
	
			else:
	
				self.logger.info(
					"Compound generated for channel %s. CompoundId: %s", 
					chan_str, eff_id
				)
