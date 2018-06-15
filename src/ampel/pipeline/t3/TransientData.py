#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 31.05.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, pymongo
from ampel.pipeline.db.DBWired import DBWired
from ampel.base.TransientView import TransientView
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.t3.DataAccessManager import DataAccessManager

class TransientData:
	"""
	"""

	al_config = None

	@classmethod
	def set_ampel_config(cls, config):
		"""
		A single TransientData instance can be used to create multiple different 
		ampel.base.TransientView instances, each representing a different channel.
		Since different channels can have different data access policies (some channel
		for example can access ZTF_COLLAB data other are restricted to ZTF_PUBLIC),
		access to the ampel config is required if multiple channel transient views
		are to be created using the same TransientData instance.
		"""
		if type(config) is pymongo.database.Database:
			TransientData.al_config = DBWired.get_config_from_db(config)
		elif type(config) is dict:
			TransientData.al_config = config
		else:
			raise ValueError("Illegal argument")


	def __init__(self, tran_id, state, logger):
		"""
		Parameters:
		* tran_id: transient id (string)
		* state: string ('$all' or '$latest') or bytes or list of md5 bytes.
		* logger: logger instance from python module 'logging'

		NOTE: in this class, dictionnaries using channel name as dict key 
		(self.compounds, self.lightcurves...) will accept *None* as dict key 
		"""
		self.tran_id = tran_id
		self.state = state
		self.logger = logger
		self.known_channels = set()
		self.flags = None

		# key: pp id (photo collection does not include channel info)
		self.photopoints = {}
		self.upperlimits = {}

		# key: channel, value: list of objets
		self.compounds = {}
		self.lightcurves = {}
		self.science_records = {}
		self.created = {}
		self.modified = {}
		self.latest_state = {}
		self.journal = {}


	def set_latest_state(self, state, channels):
		"""
		Saves latest state of transient for the provided channel
		"""
		self._set(self.latest_state, state, channels)


	def add_journal_entries(self, entries, channels=None):
		""" """
		for channel in AmpelUtils.iter(channels):
			self.known_channels.add(channel)

			for entry in entries:

				if channel in entry['channels'] or channel is None:
					shaped_entry = {k:v for k, v in entry.items() if k != 'channels'}

					if channel not in self.journal:
						self.journal[channel] = []

					if shaped_entry not in self.journal[channel]:
						self.journal[channel].append(shaped_entry)


	def set_flags(self, flags):
		""" """
		self.flags = flags


	def add_photopoint(self, photopoint):
		""" argument 'photopoint' must be an instance of ampel.base.PhotoPoint """
		self.photopoints[photopoint.get_id()] = photopoint


	def add_upperlimit(self, upperlimit):
		""" argument 'upperlimit' must be an instance of ampel.base.UpperLimit """
		self.upperlimits[upperlimit.get_id()] = upperlimit


	def add_science_record(self, science_record, channels=None):
		"""
		Saves science record and tag it with provided channels
		channels: list of strings whereby each element is a string channel id 
		science_record: instance of ampel.base.ScienceRecord
		"""
		self._add(self.science_records, science_record, channels)


	def add_lightcurve(self, lightcurve, channels=None):
		"""
		Saves lightcurve and tag it with provided channels
		channels: list of strings whereby each element is a string channel id 
		lightcurve: instance of ampel.base.LightCurve
		"""
		self._add(self.lightcurves, lightcurve, channels)


	def add_compound(self, compound, channels=None):
		"""
		Saves compound and tag it with provided channels
		channels: list of strings whereby each element is a string channel id 
		"""
		self._add(self.compounds, compound, channels)


	def create_view(self, channel=None, channels=None, t2_ids=None):
		"""
		Returns instance of ampel.base.TransientView
		"""

		photopoints, upperlimits = self._get_photo(channel, channels)

		#############################################
		# Special case 1: Create transient based on #
		# info combined from different channels.    #
		#############################################

		if channels is not None:

			# Robustness
			if type(channels) is str:
				raise ValueError("Illegal argument")

			# Someone did set strange parameter values
			if len(channels) == 1:
				return self.create_view(channel=next(iter(channels)), t2_ids=t2_ids)

			self.logger.info("Creating multi-channel transient instance")
			all_comps = self._get_combined_elements(self.compounds, channels)

			if self.state in ["$latest", "$all"]:
				latest_state = (
					TransientData.get_latest_compound(all_comps).get_id() 
					if len(all_comps) > 0 else None
				)
			else:
				latest_state = None

			# Get journal combined entries
			entries = []
			for channel in channels:
				if channel in self.journal:
					for entry in self.journal[channel]:
						if entry not in entries:
							entries.append(entry)

			return TransientView(
				self.tran_id, self.flags, None, None, # created, modified
				entries, latest_state, photopoints, upperlimits, all_comps,
				self._get_combined_elements(self.lightcurves, channels),
				self._get_combined_elements(self.science_records, channels),
				channels, self.logger
			)


		#############################################################################
		# Option 1: Unspecified channel. Channel info might not have been specified #
		# when lightcurves/compounds/... were added to this TransientData instance. #
		# In this case we have a single view TransientData (where channel==None)    #
		#############################################################################
		
		# channel not specified in create_transient(...) parameters
		if channel is None:

			if len(self.known_channels) == 0:
				if len(self.photopoints) == 0 and len(self.upperlimits) ==0:
					self.logger.warning("Returned transient will be empty!")

			# At least some lightcurves/compounds/science docs were added
			elif len(self.known_channels) == 1:

				# Someone did set inadequate parameters 
				# channel(s) were specified when compounds/... were added
				# NOTE: *None* is used as dict key, so in can be in self.known_channels
				if None not in self.known_channels:
					self.logger.warning(
						"Correcting incomplete argument: returning transient for channel %s" % 
						next(iter(self.known_channels))
					)
	
					return self.create_view(
						channel=next(iter(self.known_channels)), t2_ids=t2_ids
					)

			elif len(self.known_channels) > 1:

				# provided channel parameter is None and self.known_channels contains more than 1 channel
				raise ValueError(
					"Error: current transient view contains multi-channel information. "+
					"If your goal is to create a multi-channel transient instance, "+
					"please specifically use the argument 'channels' to do so. " +
					"Otherwise, please specify which channel info should be used to " +
					"create the transient instance by setting a value for parameter 'channel'." 
				)

		else:

			if channel not in self.known_channels:
				self.logger.debug("No transient data avail for channel %s" % channel)
				return None

		#########################################################################
		# Option 2: the most commonly used probably. Channel was specificied.   #
		# A single channel transient instance will be created whereby attention #
		# must be paid to data access rights if this TransientData contains     #
		# multi-channel transient data                                          #
		#########################################################################

		if self.state == "$all":
			if channel in self.compounds:
				latest_state = TransientData.get_latest_compound(self.compounds[channel]).get_id()
			else:
				latest_state = None	
		else:
			latest_state = self.latest_state[channel] if channel in self.latest_state else None

		return TransientView(
			self.tran_id, self.flags, 
			self.created.get(channel), 
			self.modified.get(channel), 
			self.journal.get(channel), latest_state, photopoints, upperlimits, 
			tuple(self.compounds[channel]) if channel in self.compounds else None, 
			tuple(self.lightcurves[channel]) if channel in self.lightcurves else None, 
			tuple(self.science_records[channel]) if channel in self.science_records else None, 
			channel, self.logger
		)	


	def _add(self, var, obj, channels):
		""" """
		for channel in AmpelUtils.iter(channels):
			self.known_channels.add(channel)
			if channel not in var:
				var[channel] = [obj]
			else:
				var[channel].append(obj)


	def _set(self, var, obj, channels):
		""" """
		for channel in AmpelUtils.iter(channels):
			self.known_channels.add(channel)
			var[channel] = obj


	def _get_combined_elements(self, var, channels):
		""" """
		return tuple(
			{el for channel in channels if channel in var for el in var[channel]}
		)


	def _get_photo(self, channel=None, channels=None):
		""" """

		# Robustness
		if type(channels) is str:
			raise ValueError("Illegal argument")

		# no photometric info were requested / loaded
		if len(self.photopoints) == 0 and len(self.upperlimits) == 0:
			return None, None

		# no channel info provided: we return everything.
		# The caller should have had specified a channel if he wanted 
		# to enforce data access policies
		if channel is None and channels is None:
			return tuple(self.photopoints), tuple(self.upperlimits)

		# Robustness
		if channel is not None and channels is not None:
			raise ValueError("Please choose between parameter channel and channels")

		# Past this point, regardless of how many channel info we have (one is enough) 
		# we have to check permissions (db results contain all pps/uls, 
		# wether or not they are public/private)
		if TransientData.al_config is None:
			raise ValueError(
				"Ampel config required, please set it using TransientData.set_ampel_config()"
			)
	
		# Convenience
		if channels is not None and len(channels) == 1:
			channel = next(iter(channels))

		# Get pps / uls for given channel
		if channel is not None:
			dam = DataAccessManager(TransientData.al_config, channel)
			return dam.get_photopoints(self.photopoints), dam.get_upperlimits(self.upperlimits)

		# Loop through channel(s)
		pps = set()
		uls = set()
		for channel in channels:
			dam = DataAccessManager(TransientData.al_config, channel)
			pps.update(dam.get_photopoints(self.photopoints))
			uls.update(dam.get_upperlimits(self.upperlimits))

		# Return pps / uls for given combination of channels.
		# Example for single source ZTF:
		# 1 ZTF pub chan + 1 ZTF priv chan -> priv+pub ZTF pps/uls returned
		# 1 ZTF pub chan + 1 ZTF pub chan -> pub only ZTF pps/uls returned
		# 1 ZTF priv chan + 1 ZTF priv chan -> priv+pub ZTF pps/uls returned
		return pps, uls


	@staticmethod
	def get_latest_compound(compounds):
		""" 
		Static method. 

		From a list of compound dict instances, return the compound dict 
		that corresponds to the latest state of the transient.
		Finding this out is not as obvious as it may appear at first.

		The same selection algorithm is also implemented in the method:
			DBQueryBuilder.latest_compound_query(tran_id) 
		which is evaluated by the mongoDB database aggregation framework
		(instead of locally by Python based on DB query results 
		as is the case for the present method).
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
