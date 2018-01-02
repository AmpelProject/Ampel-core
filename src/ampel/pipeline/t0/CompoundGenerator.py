#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/CompoundGenerator.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 02.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, hashlib, json
from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from ampel.pipeline.t0.dispatchers.AbstractAmpelDispatcher import AbstractAmpelDispatcher
from ampel.pipeline.t0.stampers.ZIPhotoPointStamper import ZIPhotoPointStamper
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils


HAS_HUMBOLDT_ZP = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.HAS_HUMBOLDT_ZP)
HAS_WEIZMANN_SUB = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.HAS_WEIZMANN_SUB)
SUPERSEEDED = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.SUPERSEEDED)
ZTF_PARTNERSHIP = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.ZTF_PARTNERSHIP)
SRC_T1 = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.SRC_T1)

logger = logging.getLogger("Ampel")

class CompoundGenerator():
	"""
	"""

	channel_options = dict()
	flags_to_check = {SUPERSEEDED}


	@classmethod
	def cm_add_channel_options(cls, channel_flag, partnership, arg_channel_options):

		if channel_flag in cls.channel_options:
			del cls.channel_options[channel_flag]

		cls.channel_options[channel_flag] = arg_channel_options.copy()
		cls.channel_options[channel_flag]['partnership'] = partnership

		if partnership is False:
			cls.flags_to_check.add(ZTF_PARTNERSHIP)

		if arg_channel_options['autoComplete'] is False:
			cls.flags_to_check.add(SRC_T1)

		if arg_channel_options['updatedHUZP'] is True:
			cls.flags_to_check.add(HAS_HUMBOLDT_ZP)

		if arg_channel_options['weizmannSub'] is True:
			cls.flags_to_check.add(HAS_WEIZMANN_SUB)


	def __init__(self, pps_db, ids_db, ids_alert):

		self.ids_db = ids_db
		self.ids_alert = ids_alert
		self.ids_excluded = dict()
		self.d_alflag_setofids = dict()
		self.compounds = dict()

		# Create sets for channel dependant exclusions
		for flag in CompoundGenerator.flags_to_check:
			self.d_alflag_setofids[flag] = set()

		self.d_alflag_setofids[SUPERSEEDED] = set()

		for pp in pps_db:

			for flag in CompoundGenerator.flags_to_check:
				if flag in pp['alFlags']:
					self.d_alflag_setofids[flag].add(pp['_id'])

			if "alExcluded" in pp:
				for channel in pp["alExcluded"]:
					if not channel in self.ids_excluded:
						self.ids_excluded[channel] = set()
					self.ids_excluded[channel].add(pp['_id'])
			

	def add_newly_superseeded_id(self, superseeded_id):
		self.d_alflag_setofids[SUPERSEEDED].add(superseeded_id)


	def set_db_inserted_ids(self, ids_inserted):
		self.ids_inserted = ids_inserted


	def generate(self, channel_flag):
		"""
		"""

		# Check for identical previously generated compound 
		# (several channels can use the same config parameters)
		#######################################################

		options = CompoundGenerator.channel_options[channel_flag]
		options_id = ""
		for key in sorted(options.keys()):
			options_id += str(options[key])

		if options_id in self.compounds:
			return self.compounds[options_id]


		# Generate compoundId, flavor and compound
		##########################################

		# Create compoundId
		compound = []
		compound_hash_payload = ""
		flavor_hash_payload = ""

		# Sort photopoints by id (long value)
		for pp_id in sorted(self.ids_db.union(self.ids_alert)):

			d = {'pp': pp_id}

			#  Check for exclusions
			if pp_id in self.d_alflag_setofids[SUPERSEEDED]:
				d['excl'] = SUPERSEEDED
			elif options['partnership'] is False and pp_id in self.d_alflag_setofids[ZTF_PARTNERSHIP]:
				d['excl'] = ZTF_PARTNERSHIP
			elif options['autoComplete'] is False and pp_id in self.d_alflag_setofids[SRC_T1]:
				d['excl'] = SRC_T1
			elif pp_id in self.ids_excluded[channel_flag]:
				d['excl'] = FlagUtils.get_flag_position_in_enumflag(channel_flag)

			#  Photopoint option: check if updated zero point should be used
			if options['updatedHUZP'] is True and pp_id in self.d_alflag_setofids[HAS_HUMBOLDT_ZP]:
				d['huZP'] = 1

			#  Photopoint option: check if alternative subtraction method should be used
			if options['weizmannSub'] is True and pp_id in self.d_alflag_setofids[HAS_WEIZMANN_SUB]:
				d['wzm'] = 1

			compound.append({'pp': pp_id})
			compound_hash_payload += pp_id + sorted(d.keys())
			flavor_hash_payload += json.dumps(d, sort_keys=True)

		compoundId = hashlib.md5(bytes(compound_hash_payload, "utf-8")).hexdigest()
		flavor = hashlib.md5(bytes(flavor_hash_payload, "utf-8")).hexdigest()

		logger.info("Generated compoundId: %s. Flavor: %s", compoundId, flavor)

		self.compounds[options_id] = dict()
		self.compounds[options_id][compoundId] = (channel_flag, flavor, compound)

		return self.compounds[options_id]
