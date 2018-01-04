#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/utils/CompoundGenerator.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 04.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, hashlib, json
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.ChannelFlags import ChannelFlags


HAS_HUMBOLDT_ZP = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.HAS_HUMBOLDT_ZP)
HAS_WEIZMANN_SUB = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.HAS_WEIZMANN_SUB)
SUPERSEEDED = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.SUPERSEEDED)
ZTF_PARTNERSHIP = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.ZTF_PARTNERSHIP)
SRC_T1 = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.SRC_T1)

logger = logging.getLogger("Ampel")

class CompoundGenerator():
	"""
		TODO: documentate code
	"""

	channel_options = dict()
	channel_options_ids = dict()
	flags_to_check = {SUPERSEEDED}


	@classmethod
	def cm_set_channels(cls, channel_flag, partnership, arg_channel_options):
		pass

	@classmethod
	def cm_add_channel_options(cls, channel_flag, partnership, arg_channel_options):

		if channel_flag in cls.channel_options:
			del cls.channel_options[channel_flag]

		cls.channel_options[channel_flag] = arg_channel_options.copy()
		cls.channel_options[channel_flag]['partnership'] = partnership

		# Build option_id (example: "10011")
		options_id = ""
		for key in sorted(cls.channel_options[channel_flag].keys()):
			options_id += "1" if cls.channel_options[channel_flag][key] else "0"

		cls.channel_options_ids[channel_flag] = options_id

		if partnership is False:
			cls.flags_to_check.add(ZTF_PARTNERSHIP)

		if arg_channel_options['autoComplete'] is False:
			cls.flags_to_check.add(SRC_T1)

		if arg_channel_options['updatedHUZP'] is True:
			cls.flags_to_check.add(HAS_HUMBOLDT_ZP)

		if arg_channel_options['weizmannSub'] is True:
			cls.flags_to_check.add(HAS_WEIZMANN_SUB)


	def __init__(self, pps_db, ids_alert=None):

		# Set of photopoint ids in the DB (list of db pps provided as func parameter)
		self.ids_db = set()

		# Channel dependant exclusions are activated by default 
		# and thus independant from channel configurations. 
		self.d_ids_excluded = dict()

		self.d_compounds = dict()
		
		self.d_compoundid_chans = dict()
		self.dd_chan_flavor = dict()
		self.dd_flavor_compound = dict()

		# Create ids sets 
		self.d_ids_sets = dict()

		# We need a set for the ids of superseeded pps
		self.d_ids_sets[SUPERSEEDED] = set()

		# flags_to_check was set during *static* init depending on 
		# the *configuration* of each *active* channel
		# We only need sets of ids for each flags_to_check since it could 
		# happen that no channel requires to check for say 'updatedHUZP'
		for flag in CompoundGenerator.flags_to_check:
			self.d_ids_sets[flag] = set()

		# Loop through provided list of pps in order to populate the different set of ids
		for pp in pps_db:

			# Build set of ids
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

			
	def get_set_of_db_pp_ids(self):
		return self.ids_db


	def add_newly_superseeded_id(self, superseeded_id):
		self.d_ids_sets[SUPERSEEDED].add(superseeded_id)


	def set_db_inserted_ids(self, ids_inserted):
		self.ids_inserted = ids_inserted


	def get_t2_docs_flavors(self, compound_id):
		"""	
		"""	
		out = []
		d = self.dd_chan_flavor[compound_id]
		for key in d:
			out.append({'flavor': d[key], 'channel': key.name})
		return out


	def get_compound_doc_flavors(self, compound_id):
		"""	
		"""	
		out = []
		d = self.dd_flavor_compound[compound_id]
		for key in d:
			out.append({'flavor': key.name, 'compound': d[key]})
		return out

	
	def get_compound_ids(self, channel_flags):

		compound_ids = set()

		for chan_flag in channel_flags.as_list():
			options_id = CompoundGenerator.channel_options_ids[chan_flag]
			compound_ids.add(self.d_compounds[options_id][0])

		return compound_ids


	def get_channels_for_compoundid(self, compound_id):
		"""
		"""
		return self.d_compoundid_chans[compound_id]


	def generate(self, channel_flag):
		"""
		"""

		#######################################################
		# Check for identical previously generated compound
		# (several channels can use the same config parameters)
		#######################################################

		options_id = CompoundGenerator.channel_options_ids[channel_flag]

		if options_id in self.d_compounds:
			
			compound_id = self.d_compounds[options_id][0]
			flavor = self.d_compounds[options_id][1]
			self.d_compoundid_chans[compound_id] |= channel_flag

			if compound_id != flavor:
				self.dd_chan_flavor[compound_id][channel_flag] = flavor
				log_output = compound_id + ", flavor: " +flavor
			else:
				log_output = compound_id 

			logger.info(
				"Re-using previoulsy generated compound for channel %s. CompoundId: %s", 
				channel_flag.name, log_output
			)

			return 

		options = CompoundGenerator.channel_options[channel_flag]


		##########################################
		# Generate compoundId, flavor and compound
		##########################################

		# Create compoundId
		compound = []
		compound_hash_payload = ""
		flavor_hash_payload = ""

		# Loop through avail pps ids	
		for pp_id in self.raw_compound_ids:

			d = {'pp': pp_id}

			#  Check for exclusions
			if pp_id in self.d_ids_sets[SUPERSEEDED]:
				d['excl'] = SUPERSEEDED
			elif options['partnership'] is False and pp_id in self.d_ids_sets[ZTF_PARTNERSHIP]:
				d['excl'] = ZTF_PARTNERSHIP
			elif options['autoComplete'] is False and pp_id in self.d_ids_sets[SRC_T1]:
				d['excl'] = SRC_T1
			elif pp_id in self.d_ids_excluded[channel_flag]:
				d['excl'] = FlagUtils.get_flag_position_in_enumflag(channel_flag)

			#  Photopoint option: check if updated zero point should be used
			if options['updatedHUZP'] is True and pp_id in self.d_ids_sets[HAS_HUMBOLDT_ZP]:
				d['huZP'] = 1

			#  Photopoint option: check if alternative subtraction method should be used
			if options['weizmannSub'] is True and pp_id in self.d_ids_sets[HAS_WEIZMANN_SUB]:
				d['wzm'] = 1

			compound.append(d)

			if len(d) == 1:
				sppid = str(pp_id)
				compound_hash_payload += sppid
				flavor_hash_payload += sppid
			else:
				flavor_hash_payload += json.dumps(d, sort_keys=True)
				if not 'excl' in d:
					compound_hash_payload += json.dumps(d, sort_keys=True)

		compound_id = hashlib.md5(bytes(compound_hash_payload, "utf-8")).hexdigest()
		flavor = hashlib.md5(bytes(flavor_hash_payload, "utf-8")).hexdigest()

		logger.info(
			"Compound generated for channel %s. CompoundId: %s, flavor: %s", 
			channel_flag.name, compound_id, flavor
		)

		self.d_compounds[options_id] = (compound_id, flavor, compound)
		self.d_compoundid_chans[compound_id] |= channel_flag

		if compound_id != flavor:
			self.dd_chan_flavor[compound_id][channel_flag] = flavor
			self.dd_flavor_compound[compound_id][flavor] = compound

