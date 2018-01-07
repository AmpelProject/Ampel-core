#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/utils/CompoundGenerator.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 07.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, hashlib, json
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.ChannelFlags import ChannelFlags


HAS_HUMBOLDT_ZP = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.HAS_HUMBOLDT_ZP)
HAS_WEIZMANN_SUB = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.HAS_WEIZMANN_SUB)
SUPERSEEDED = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SUPERSEEDED)
ZTF_PARTNERSHIP = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.ZTF_PARTNERSHIP)
SRC_T1 = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SRC_T1)

logger = logging.getLogger("Ampel")

class CompoundGenerator():
	"""
		Documentation will follow eventually
	"""

	channel_options = dict()
	channel_options_sig = dict()
	flags_to_check = {SUPERSEEDED}


	@classmethod
	def cm_init_channels(cls, channels_config, channel_names):
		"""
			Sets required internal static variables.
			For T0: channel_names must contain the active channels.
			channels_config: instance of ampel.pipeline.common.ChannelsConfig
			channel_names: list of channnel names (string) as defined in the ampel config 
						   in the DB (collection: 'config', dict key: 'channels')
		"""
		for channel_name in channel_names:
			cls.cm_init_channel(channels_config, channel_name)


	@classmethod
	def cm_init_channel(cls, channels_config, channel_name):
		"""
			Sets required internal static variables.
			(For T0: each 'active' channel must be added) 
			channels_config: instance of ampel.pipeline.common.ChannelsConfig
			channel_name: single channnel name (string) as defined in the ampel config 
						  in the DB (collection: 'config', dict key: 'channels')
		"""

		chan_flag = channels_config.get_channel_flag_instance(channel_name)

		if chan_flag in cls.channel_options:
			del cls.channel_options[chan_flag]

		# get channel parameters from ChannelsConfig instance
		cls.channel_options[chan_flag] = channels_config.get_channel_parameters(channel_name)

		# shortcut
		chan_opts = cls.channel_options[chan_flag]

		# Generate options signature (example: "10011")
		options_sig = ""
		for key in sorted(chan_opts.keys()):
			options_sig += "1" if chan_opts[key] else "0"

		# Save it as static variable
		cls.channel_options_sig[chan_flag] = options_sig

		if chan_opts['partnership'] is False:
			cls.flags_to_check.add(ZTF_PARTNERSHIP)

		if chan_opts['autoComplete'] is False:
			cls.flags_to_check.add(SRC_T1)

		if chan_opts['updatedHUZP'] is True:
			cls.flags_to_check.add(HAS_HUMBOLDT_ZP)

		if chan_opts['weizmannSub'] is True:
			cls.flags_to_check.add(HAS_WEIZMANN_SUB)


	def __init__(self, pps_db, ids_alert=None):

		# Create set for DB photopoint ids (list of db pps provided as func parameter)
		self.ids_db = set()

		# Channel based exclusions (implemented using d_ids_excluded) are a 
		# default feature of Ampel and thus independant from channel configurations. 
		# Dict key is a channel flag and the value is the associated set of photopoint ids
		self.d_ids_excluded = dict()

		# Dict with flag index pos (integer) as key (such as SRC_T1, ZTF_PARTNERSHIP, ...)
		# and the corresponding set of ids as value
		self.d_ids_sets = dict()

		# We need a set for the ids of superseeded pps
		self.d_ids_sets[SUPERSEEDED] = set()

		# flags_to_check was set during static init depending on the configuration of 
		# each added channel. We need ids sets not for every existing option but only 
		# for the flags defined in flags_to_check since it could happen that no channel 
		# has activated the option say 'updatedHUZP'
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

			# Channel specific photophoint exclusion. pp["alExcluded"] could look like this. 
			# [CHANNEL_SN, CHANNEL_GRB] (whereby each value is a flag index position rather than a flag value)
			if "alExcluded" in pp:
				for chan_flag in FlagUtils.dbflag_to_enumflag(pp["alExcluded"], ChannelFlags).as_list():
					if not chan_flag in self.d_ids_excluded:
						self.d_ids_excluded[chan_flag] = set()
					self.d_ids_excluded[chan_flag].add(pp['_id'])
			
		# Sort photopoints by id (type: long)
		if ids_alert is None:
			self.raw_compound_ids = sorted(self.ids_db)
		else:
			self.raw_compound_ids = sorted(self.ids_db.union(ids_alert))


		# 1
		self.d_effid_comp = dict()

		# 2
		self.d_effid_chanflags = dict()

		# 3
		self.d_effid_stidnchan = dict()

		# 4
		self.d_stid_compdiff = dict()

		# 5
		self.d_optsig_effid = dict()

		# 6
		self.d_optsig_stid = dict()

			
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
				'channel': FlagUtils.get_flag_pos_in_enumflag(el[0]),
				'flavor': el[1]
			} 
			for el in self.d_effid_stidnchan[compound_id]
		]


	def has_flavors(self, compound_id):
		"""	
		"""	
		if not compound_id in self.d_effid_stidnchan:
			return False

		if len(self.d_effid_stidnchan[compound_id]) == 0:
			return False

		return True


	def get_compound_flavors(self, compound_id):
		"""	
		"""	
		return [
			{'flavor': el[1], 'omitted': self.d_stid_compdiff[el[1]]}
			for el in self.d_effid_stidnchan[compound_id]
		]

	
	def get_compound_ids(self, channel_flags):

		compound_ids = set()

		for chan_flag in channel_flags.as_list():
			options_sig = CompoundGenerator.channel_options_sig[chan_flag]
			compound_ids.add(self.d_optsig_effid[options_sig])

		return compound_ids


	def get_channels_for_compoundid(self, compound_id):
		"""
		"""
		return self.d_effid_chanflags[compound_id]


	def generate(self, list_of_channel_flag):
		"""
		"""

		for channel_flag in list_of_channel_flag.as_list():

			#######################################################
			# Check for identical previously generated compound
			# (several channels can have the same config parameters)
			#######################################################
	
			chan_opts_sig = CompoundGenerator.channel_options_sig[channel_flag]
	
			if chan_opts_sig in self.d_optsig_effid:
				
				eff_id = self.d_optsig_effid[chan_opts_sig]
				self.d_effid_chanflags[eff_id] |= channel_flag
	
				if chan_opts_sig in self.d_optsig_stid:
					strict_id = self.d_optsig_stid[chan_opts_sig]
					log_output = "(eff: " + eff_id + ", strict: " + strict_id + ")"
					if not eff_id in self.d_effid_stidnchan:
						self.d_effid_stidnchan[eff_id] = set()
					self.d_effid_stidnchan[eff_id].add((channel_flag, strict_id))
				else:
					log_output = eff_id 
	
				logger.info(
					"Using previoulsy generated compound for channel %s. CompoundId: %s", 
					channel_flag.name, log_output
				)
	
				return 
	
	
			##########################################
			# Generate compoundId, flavor and compound
			##########################################
	
			eff_comp = []
			strict_comp = []
			eff_hash_payload = ""
			strict_hash_payload = ""
			chan_options = CompoundGenerator.channel_options[channel_flag]
	
			# Create compound and compoundId
			for pp_id in self.raw_compound_ids:
	
				d = {'pp': pp_id}
	
				#  Check for exclusions
				if pp_id in self.d_ids_sets[SUPERSEEDED]:
					d['excl'] = SUPERSEEDED
				elif chan_options['partnership'] is False and pp_id in self.d_ids_sets[ZTF_PARTNERSHIP]:
					d['excl'] = ZTF_PARTNERSHIP
				elif chan_options['autoComplete'] is False and pp_id in self.d_ids_sets[SRC_T1]:
					d['excl'] = SRC_T1
				elif channel_flag in self.d_ids_excluded and pp_id in self.d_ids_excluded[channel_flag]:
					d['excl'] = FlagUtils.get_flag_pos_in_enumflag(channel_flag)
	
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
	
			eff_id = hashlib.md5(bytes(eff_hash_payload, "utf-8")).hexdigest()
			strict_id = hashlib.md5(bytes(strict_hash_payload, "utf-8")).hexdigest()
	
			
			##################
			# Record results #
			##################
	
			if eff_id in self.d_effid_chanflags:
				self.d_effid_chanflags[eff_id] |= channel_flag
			else:
				self.d_effid_chanflags[eff_id] = channel_flag

			self.d_effid_comp[eff_id] = eff_comp
			self.d_optsig_effid[chan_opts_sig] = eff_id
	
			if eff_id != strict_id:
	
				logger.info(
					"Compound generated for channel %s. CompoundId: (eff: %s, strict: %s)", 
					channel_flag.name, eff_id, strict_id
				)
	
				self.d_optsig_stid[chan_opts_sig] = strict_id
				if not eff_id in self.d_effid_stidnchan:
					self.d_effid_stidnchan[eff_id] = set()
				self.d_effid_stidnchan[eff_id].add((channel_flag, strict_id))
				self.d_stid_compdiff[strict_id] = strict_comp
	
			else:
	
				logger.info(
					"Compound generated for channel %s. CompoundId: %s", 
					channel_flag.name, eff_id
				)
