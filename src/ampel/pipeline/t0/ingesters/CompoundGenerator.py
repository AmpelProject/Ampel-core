#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/CompoundGenerator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 18.04.2018
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
SRC_T1 = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SRC_T1)


class CompoundGenerator():
	"""
	This class requires documentation to be understood.
	I'll to do that in the near future (have it on paper)
	"""

	channel_options = {}
	channel_options_sig = {}
	flags_to_check = {SUPERSEEDED}
	ChannelFlags = None


	@classmethod
	def set_ChannelFlags(cls, arg):
		CompoundGenerator.ChannelFlags = arg


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

		chan_int = channel.get_flag().value

		# In case this method is called multiple times
		if chan_int in cls.channel_options:
			del cls.channel_options[chan_int]

		# Input parameters example:
		# { "ZTFPartner" : true, "autoComplete" : true, "updatedHUZP" : false }
		cls.channel_options[chan_int] = channel.get_input().get_parameters()

		# shortcut
		chan_opts = cls.channel_options[chan_int]

		# Generate options signature (example: "10011")
		options_sig = ""
		for key in sorted(chan_opts.keys()):
			options_sig += "1" if chan_opts[key] else "0"

		# Save it as static variable
		cls.channel_options_sig[chan_int] = options_sig

		# flags_to_check is used for optimization of the init routine
		if chan_opts['ZTFPartner'] is False:
			cls.flags_to_check.add(ZTF_COLLAB)

		if chan_opts['autoComplete'] is False:
			cls.flags_to_check.add(SRC_T1)

		if "updatedHUZP" in chan_opts and chan_opts['updatedHUZP'] is True:
			cls.flags_to_check.add(HAS_HUMBOLDT_ZP)

		if "weizmannSub" in chan_opts and chan_opts['weizmannSub'] is True:
			cls.flags_to_check.add(HAS_WEIZMANN_SUB)


	def __init__(self, pps_db, logger, ids_alert=None):
		"""
		"""

		self.logger = logger

		# Create set for DB photopoint ids (list of db pps provided as func parameter)
		self.ids_db = set()

		# Channel based exclusions (implemented using d_ids_excluded) are a 
		# default feature of Ampel and thus independant from channel configurations. 
		# Dict key is a channel flag and the value is the associated set of photopoint ids
		self.d_ids_excluded = dict()

		# Dict with flag index pos (integer) as key (such as SRC_T1, ZTF_COLLAB, ...)
		# and the corresponding set of ids as value
		self.d_ids_sets = dict()

		# We need a set for the ids of superseeded pps
		self.d_ids_sets[SUPERSEEDED] = set()

		# flags_to_check was set during static init depending on the configuration of 
		# each added channel. We need sets of ids not for every existing option but only 
		# for the flags defined in flags_to_check since it can happen that no channel 
		# uses the option say 'updatedHUZP'
		for flag in CompoundGenerator.flags_to_check:
			self.d_ids_sets[flag] = set()

		# Loop through provided list of pps in order to populate the different set of ids
		for pp in pps_db:

			# Build set of DB ids
			self.ids_db.add(pp["_id"])

			# Check photopoint flags defined in flags_to_check and populate associated sets
			for flag in CompoundGenerator.flags_to_check:
				if flag in pp['alFlags']:
					self.d_ids_sets[flag].add(pp['_id'])

			# Channel specific photophoint exclusion. pp["alExcluded"] could look like this: 
			# [CHANNEL_SN, CHANNEL_GRB]
			if "alExcluded" in pp:
				for chan_str in pp["alExcluded"]:
					chan_int = CompoundGenerator.ChannelFlags[chan_str].value
					if not chan_int in self.d_ids_excluded:
						self.d_ids_excluded[chan_int] = set()
					self.d_ids_excluded[chan_int].add(pp['_id'])
			
		# Sort photopoints by id (type: long)
		if ids_alert is None:
			self.pp_ids = sorted(self.ids_db)
		else:
			self.pp_ids = sorted(self.ids_db.union(ids_alert))


		# 1
		self.d_effid_comp = dict()

		# 2
		self.d_effid_chanflags = dict()

		# 3
		self.d_effid_strictid_plus_chan = dict()

		# 4
		self.d_stid_compdiff = dict()

		# 5
		self.d_optionsig_effid = dict()

		# 6
		self.d_optionsig_strictid = dict()

			
	def get_db_ids(self):
		""" 
		Returns a python set containing the "_id" of every photopoint contained in the DB
		"""
		return self.ids_db


	def add_newly_superseeded_id(self, superseeded_id):
		self.d_ids_sets[SUPERSEEDED].add(superseeded_id)


	def get_eff_compound(self, compound_id):
		return self.d_effid_comp[compound_id]


	def get_t2_flavors(self, compound_id):
		"""	
		"""	
		return [
			{
				'channel': [ell.name for ell in el[0].as_list()],
				'flavor': el[1]
			} 
			for el in self.d_effid_strictid_plus_chan[compound_id]
		]


	def has_flavors(self, compound_id):
		"""	
		"""	
		if not compound_id in self.d_effid_strictid_plus_chan:
			return False

		if len(self.d_effid_strictid_plus_chan[compound_id]) == 0:
			return False

		return True


	def get_compound_flavors(self, compound_id):
		"""	
		"""	
		return [
			{'flavor': el[1], 'omitted': self.d_stid_compdiff[el[1]]}
			for el in self.d_effid_strictid_plus_chan[compound_id]
		]

	
	def get_compound_ids(self, channel_flags):

		compound_ids = set()

		for chan_flag in channel_flags.as_list():
			options_sig = CompoundGenerator.channel_options_sig[chan_flag.value]
			compound_ids.add(self.d_optionsig_effid[options_sig])

		return compound_ids


	def get_channels_for_compoundid(self, compound_id):
		"""
		"""
		return self.d_effid_chanflags[compound_id]


	def generate(self, chan_flags):
		"""
		"""

		for chan_flag in chan_flags.as_list():

			#######################################################
			# Check for identical previously generated compound
			# (several channels can have the same config parameters)
			#######################################################
	
			# Get channel option signature (ex: "01101011")
			chan_opts_sig = CompoundGenerator.channel_options_sig[chan_flag.value]
	
			# if, given the current channel options, effective id was already computed 
			if chan_opts_sig in self.d_optionsig_effid:

				# Retrieve previously computed effective id
				eff_id = self.d_optionsig_effid[chan_opts_sig]

				# Add current flag to the flags associated with this effective id
				self.d_effid_chanflags[eff_id] |= chan_flag
	
				# if a strict id exists for the current channel options
				if chan_opts_sig in self.d_optionsig_strictid:

					# Retrieve strict id
					strict_id = self.d_optionsig_strictid[chan_opts_sig]

					# Feedback
					log_output = "(eff: " + eff_id + ", strict: " + strict_id + ")"

					# Add tupple (chan_flag, strict id) to internal dict using eff_id as key
					if not eff_id in self.d_effid_strictid_plus_chan:
						self.d_effid_strictid_plus_chan[eff_id] = set()
					self.d_effid_strictid_plus_chan[eff_id].add((chan_flag, strict_id))
				else:
					log_output = eff_id 
	
				self.logger.info(
					"Using previoulsy generated compound for channel %s. CompoundId: %s", 
					chan_flag.name, log_output
				)
	
				continue 
	
	
			##########################################
			# Generate compoundId, flavor and compound
			##########################################
	
			eff_comp = []
			strict_comp = []
			eff_hash_payload = ""
			strict_hash_payload = ""
			chan_options = CompoundGenerator.channel_options[chan_flag.value]
	
			# Create compound and compoundId
			for pp_id in self.pp_ids:
	
				d = {'pp': pp_id}
	
				#  Check for exclusions
				if pp_id in self.d_ids_sets[SUPERSEEDED]:
					d['excl'] = SUPERSEEDED
				elif chan_options['ZTFPartner'] is False and pp_id in self.d_ids_sets[ZTF_COLLAB]:
					d['excl'] = ZTF_COLLAB
				elif chan_options['autoComplete'] is False and pp_id in self.d_ids_sets[SRC_T1]:
					d['excl'] = SRC_T1
				elif chan_flag in self.d_ids_excluded and pp_id in self.d_ids_excluded[chan_flag.value]:
					d['excl'] = chan_flag.name
	
				#  Photopoint option: check if updated zero point should be used
				if chan_options['updatedHUZP'] is True and pp_id in self.d_ids_sets[HAS_HUMBOLDT_ZP]:
					d['huZP'] = 1
	
				#  Photopoint option: check if alternative subtraction method should be used
				if chan_options['weizmannSub'] is True and pp_id in self.d_ids_sets[HAS_WEIZMANN_SUB]:
					d['wzm'] = 1
	
				eff_comp.append(d)
	
				if len(d) == 1:
					sppid = str(pp_id)
					eff_hash_payload += sppid
					strict_hash_payload += sppid
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
	
			# Save channel flag to dict using eff_id as key
			if eff_id in self.d_effid_chanflags:
				self.d_effid_chanflags[eff_id] |= chan_flag
			else:
				self.d_effid_chanflags[eff_id] = chan_flag

			self.d_effid_comp[eff_id] = eff_comp
			self.d_optionsig_effid[chan_opts_sig] = eff_id
	
			if eff_id != strict_id:
	
				self.logger.info(
					"Compound generated for channel %s. CompoundId: (eff: %s, strict: %s)", 
					chan_flag.name, eff_id, strict_id
				)
	
				# Save strict id using channel option signature as key
				self.d_optionsig_strictid[chan_opts_sig] = strict_id

				# Add tupple (chan_flag, strict id) to internal dict using eff_id as key
				if not eff_id in self.d_effid_strictid_plus_chan:
					self.d_effid_strictid_plus_chan[eff_id] = set()
				self.d_effid_strictid_plus_chan[eff_id].add((chan_flag, strict_id))

				self.d_stid_compdiff[strict_id] = strict_comp
	
			else:
	
				self.logger.info(
					"Compound generated for channel %s. CompoundId: %s", 
					chan_flag.name, eff_id
				)
