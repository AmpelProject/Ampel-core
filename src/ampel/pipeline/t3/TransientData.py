#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.05.2018
# Last Modified Date: 15.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.TransientView import TransientView
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.t3.PhotoDataAccessManager import PhotoDataAccessManager

class TransientData:
	"""
	"""

	# Static dict with key=channel, value=instance of PhotoDataAccessManager
	pdams = {}

	def __init__(self, tran_id, state, logger):
		"""
		:param int tran_id: transient id
		:param state: string ('$all' or '$latest') or bytes or list of md5 bytes.
		:param logger: logger instance from python module 'logging'

		NOTE: in this class, dictionnaries using channel name as dict key 
		(self.compounds, self.lightcurves...) will accept *None* as dict key 
		"""
		self.tran_id = tran_id
		self.state = state
		self.logger = logger

		# key: pp id (photo collection does not include channel info)
		self.photopoints = {}
		self.upperlimits = {}

		# key: channel, value: list of objets
		self.compounds = {}
		self.lightcurves = {}
		self.science_records = {}
		self.latest_state = {}
		self.journal = {}


	def set_channels(self, channels):
		""" 
		:param set(str) channels: set of channel names
		"""
		self.channels = channels


	def set_flags(self, flags):
		"""
		:param flags: enum flag instance of :py:class:`TransientFlags `<ampel.base.flags.TransientFlags>`
		"""
		self.flags = flags


	def set_latest_state(self, channels, state):
		""" Saves latest state of transient for the provided channel """
		self._set(self.latest_state, state, channels)


	def add_photopoint(self, photopoint):
		""" 
		:type photopoint: :py:class:`PhotoPoint <ampel.base.PhotoPoint>`
		"""
		self.photopoints[photopoint.content["_id"]] = photopoint


	def add_upperlimit(self, upperlimit):
		"""
		:type upperlimit: :py:class:`UpperLimit <ampel.base.UpperLimit>`
		"""
		self.upperlimits[upperlimit.content["_id"]] = upperlimit


	def add_science_record(self, channels, science_record):
		"""
		Saves science record and tag it with provided channels

		:param list(str) channels: list of strings whereby each element is a channel id 
		:type science_record: :py:class:`ScienceRecord <ampel.base.ScienceRecord>`
		"""
		self._add(self.science_records, science_record, channels)


	def add_lightcurve(self, channels, lightcurve):
		"""
		Saves lightcurve and tag it with provided channels

		:param list(str) channels: list of strings whereby each element is a channel id 
		:type lightcurve: :py:class:`ScienceRecord <ampel.base.lightcurve>`
		"""
		self._add(self.lightcurves, lightcurve, channels)


	def add_compound(self, channels, compound):
		"""
		Saves compound and tag it with provided channels

		:param list(str) channels: list of strings whereby each element is a channel id 
		:param dataclass compound: instance of :py:class:`Compound <ampel.base.Compound>`
		"""
		self._add(self.compounds, compound, channels)


	def add_journal_entry(self, channels, entry):
		""" 
		:param channels: list/set of strings whereby each element is a string channel id 
		:param dict entry: journal entry
		"""
		self._add(self.journal, entry, channels)


	# TODO: implement docs filtering
	def create_view(self, channels, docs=None, t2_ids=None):
		"""
		:param channels: channel names
		:type channels: sequence, set(str)
		:returns: instance of :py:class:`TransientView <ampel.base.TransientView>` or None
		"""

		# Create transient based on info combined from different channels.
		if AmpelUtils.is_sequence(channels):
			return self._create_multi_view(channels, docs, t2_ids)

		if isinstance(channels, set):
			if len(channels) == 1:
				return self._create_one_view(next(iter(channels)), docs, t2_ids)
			return self._create_multi_view(channels, docs, t2_ids)

		# Unspecified channel. We create a view based on what's available
		if channels is None:

			# No lightcurve/science record/compound associated with this transient 
			if not self.channels:
				raise ValueError("TransientData not associated with any channel")

			# At least one channel association exisits
			elif len(self.channels) == 1:
				return self._create_one_view(
					next(iter(self.channels)), docs, t2_ids
				)

			else:
				self.logger.warning("Creating multi-view transient with all available channels")
				return self._create_multi_view(self.channels, docs, t2_ids)

		# Channel was specificied (channels is actually a channel)
		return self._create_one_view(channels, docs, t2_ids)


	# TODO: implement docs filtering
	def _create_one_view(self, channel, docs=None, t2_ids=None):
		"""
		:returns: instance of :py:class:`TransientView <ampel.base.TransientView>` or None
		"""

		if not type(channel) is str:
			raise ValueError("type(channel) must be str and not %s" % type(channel))

		if channel not in self.channels:
			self.logger.debug("No transient data avail for %s and channel(s) %s" % 
				(self.tran_id, str(channel))
			)
			return None

		if self.state == "$all":
			if channel in self.compounds:
				latest_state = TransientData.get_latest_compound(self.compounds[channel]).get_id()
			else:
				latest_state = None	
		else:
			latest_state = self.latest_state[channel] if channel in self.latest_state else None

		# Handles data permission
		photopoints, upperlimits = self._get_photo(channel)

		return TransientView(
			self.tran_id, self.flags, self.journal.get(channel), 
			latest_state, photopoints, upperlimits, 
			tuple(self.compounds[channel]) if channel in self.compounds else None, 
			tuple(self.lightcurves[channel]) if channel in self.lightcurves else None, 
			tuple(self.science_records[channel]) if channel in self.science_records else None, 
			channel
		)	


	# TODO: implement docs filtering
	def _create_multi_view(self, channels, docs=None, t2_ids=None):
		"""
		:returns: instance of :py:class:`TransientView <ampel.base.TransientView>` or None
		"""

		# Sequence with single value
		if len(channels) == 1:
			return self._create_one_view(next(iter(channels)), docs, t2_ids)

		# Gather compounds from different channels 
		# (will be empty of coumpound loading was not requested)
		all_comps = {
			el.id: el for channel in AmpelUtils.iter(channels) if channel in self.compounds 
			for el in self.compounds[channel]
		}

		# State operator was provided
		if self.state in ["$latest", "$all"]:
			latest_state = (
				TransientData.get_latest_compound(list(all_comps.values())).id
				if len(all_comps) > 0 else None
			)
		# Custom/dedicated state(s) was/were provided
		else:
			latest_state = None

		# combined journal entries for provided channels
		entries = []
		for channel in AmpelUtils.iter(channels):
			if channel in self.journal:
				for entry in self.journal[channel]:
					if entry not in entries:
						entries.append(entry)

		# Handles data permission
		photopoints, upperlimits = self._get_photo(channels)

		return TransientView(
			self.tran_id, self.flags, entries, latest_state, photopoints, upperlimits, 
			tuple(all_comps.values()),
			self._get_combined_elements(self.lightcurves, channels),
			self._get_combined_elements(self.science_records, channels),
			tuple(self.channels & set(channels))
		)


	def _add(self, var, obj, channels):
		""" """
		for channel in AmpelUtils.iter(channels):
			if channel not in var:
				var[channel] = [obj]
			else:
				var[channel].append(obj)


	def _set(self, var, obj, channels):
		""" """
		for channel in AmpelUtils.iter(channels):
			var[channel] = obj


	def _get_combined_elements(self, var, channels):
		""" """
		elements = [
			el for channel in AmpelUtils.iter(channels) 
			if channel in var 
			for el in var[channel]
		]

		try:
			return tuple(set(elements))
		except TypeError:
			# if contained elements are not hashable, revert to identity
			return tuple({id(k): k for k in elements}.values())


	def _get_photo(self, channels):
		""" """

		# no photometric info were requested / loaded
		if len(self.photopoints) == 0 and len(self.upperlimits) == 0:
			return None, None

		# no channel info provided: we return everything.
		# The caller should have specified a channel 
		# if data access policies were to be applied
		if channels is None:
			return tuple(self.photopoints), tuple(self.upperlimits)

		# Past this point, regardless of how many channel info we have (one is enough) 
		# we have to check permissions (db results contain all pps/uls, 
		# wether or not they are public/private)
	
		# Loop through channel(s)
		pps = set()
		uls = set()
		for channel in AmpelUtils.iter(channels):
			if channel not in TransientData.pdams:
				TransientData.pdams[channel] =  PhotoDataAccessManager(channel)
			pps.update(TransientData.pdams[channel].get_photopoints(self.photopoints))
			uls.update(TransientData.pdams[channel].get_upperlimits(self.upperlimits))

		# Return pps / uls for given combination of channels.
		# Example for single source ZTF:
		# 1 ZTF pub chan + 1 ZTF priv chan -> priv+pub ZTF pps/uls returned
		# 1 ZTF pub chan + 1 ZTF pub chan -> pub only ZTF pps/uls returned
		# 1 ZTF priv chan + 1 ZTF priv chan -> priv+pub ZTF pps/uls returned
		return pps, uls


	@staticmethod
	def get_latest_compound(compounds):
		""" 
		| Static method which from a list of compound dict instances, \
		returns the compound dict that corresponds to the latest state of the transient.
		| Finding this out is not as obvious as it may appear.

		The same selection algorithm is also implemented in the method: \
		:func:`QueryLatestCompound.general_query(...)` which is evaluated \
		by the mongoDB aggregation framework (instead of locally by python \
		based on DB query results as is the case for the present method).
		"""

		# Check exit
		if len(compounds) == 1:
			return compounds[0]


		# 1) sort compounds by added date
		#################################

		date_added_sorted_comps = sorted(
			compounds, key=lambda x: x.added, reverse=True
		)


		# 2) group first elements with same source (src) 
		# and consider only the first group
		################################################

		ref_tier = date_added_sorted_comps[0].tier
		first_group_comps = []
		for comp in date_added_sorted_comps:
			if comp.tier == ref_tier:
				first_group_comps.append(comp)
			else:
				break
		
		# Check exit
		if len(first_group_comps) == 1:
			return first_group_comps[0]


		# 3) Implement tier based sort strategy
		#######################################
		
		# T0: return compound with latest pp date
		if ref_tier == 0:

			lastppdt_sorted_lcs = sorted(
				first_group_comps, key=lambda x: x.len, reverse=True
			)

			return lastppdt_sorted_lcs[0]

		# T1 or T3: return first element (newest added date)
		elif ref_tier == 1 or ref_tier == 3:
			return first_group_comps[0]
	
		else:
			raise NotImplementedError(
				"Sort algorithm not implemented for tier %i" % ref_tier
			)
