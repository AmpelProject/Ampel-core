#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/CompoundBluePrint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 08.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json


class CompoundBluePrint():
	"""
	This class requires documentation to be understood.
	I'll try to do that in the future.
	Name shortcuts used in this class:
	"eid": effective id
	"sid": strict id
	"chan": channel name
	"flagpos": index position of flag
	"comp": compound
	"opt": option
	"""

	def __init__(self, comp_el, logger):
		"""
		comp_el: instance of child class of ampel.abstract.AbsCompElement
		logger: logger instance (python module 'logging')
		"""

		self.logger = logger
		self.comp_el = comp_el


	def generate(self, dict_list, channel_names):
		"""	
		set of channel names (string)
		dict_list: list of dict instances representing photopoint or upperlimis measurements
		######################################
		IMPORTANT: list must be timely sorted.
		######################################
		The sorting is not done within this method because CompoundBluePrint 
		aims at being instrument independant.  For ZTF, the field 'jd' is used:
		-> sorted(dict_list, key=lambda k: k['jd'])
		"""	

		#1 save compound lists using effective id as key
		self.d_eid_comp = {}

		#2 save channels names using effective id as key
		self.d_eid_chnames = {}

		#3 save channels names using pp id as key
		self.d_ppid_chnames = {}

		#4 save tuple (chan name, strict id) using effective id as key
		self.d_eid_tuple_chan_sid = {}

		#5 save strict compound difference (wrt to effective compound) using strict if as key 
		self.d_sid_compdiff = {}

		#6 save eid <-> ppid association
		self.d_eid_ppid = {}

		# Main loop through provided channels
		for chan_name in channel_names:

			#########################################
			# Generate compounds, md5s, and flavors #
			#########################################
	
			eff_comp = []
			strict_comp = []
			eff_hash_payload = ""
			strict_hash_payload = ""
			pp_hash_payload = ""
			gen_dict = self.comp_el.gen_dict
	
			# Create compound and compoundId
			for el in dict_list:
	
				# Generate compound entry dictionary
				comp_entry = gen_dict(el, chan_name)	

				# Append generated dict to effective compound list
				eff_comp.append(comp_entry)

				# Len == 1 means not policy or exclusion
				if len(comp_entry) == 1:

					# pps or uls ids are long (ZTF-IPAC)
					dict_str = str(comp_entry) 

					# append string repr of dict to effective and strict payloads
					eff_hash_payload += dict_str
					strict_hash_payload += dict_str

					# pp payload is updated only if input is not an upper limit 
					# (featuring key 'ul' rather than 'pp')
					if 'pp' in comp_entry: 
						pp_hash_payload += dict_str

				# Check if policy or exclusion
				# In both case, strict id payload must be appended
				else:
					
					# build str repr of dict. sort_keys is important !
					dict_str = json.dumps(comp_entry, sort_keys=True)

					# Append dict string to strict payload
					strict_hash_payload += dict_str

					# Update strict compound
					strict_comp.append(comp_entry)

					# photopoint or upper limit has a custom policy
					if not 'excl' in comp_entry:

						# For this class, a PP/UL defined with policy is just like 
						# a different PP/UL (meaning a PP/UL with a different ID). 
						# They might return indeed different magnitudes.
						# PPS/ULS with policies will result in a different effective payload 
						# and thus a different effective id
						eff_hash_payload += dict_str

						# Update photopoint payload if input is not an upper limit
						if 'pp' in comp_entry: 
							pp_hash_payload += dict_str

	
			# eff_id = effective compound id = md5 hash of effective payload
			eff_id = hashlib.md5(bytes(eff_hash_payload, "utf-8")).hexdigest()

			# strict_id = strict compound id = md5 hash of strict payload
			strict_id = hashlib.md5(bytes(strict_hash_payload, "utf-8")).hexdigest()
	
			# pp_id = photopoints compound id = md5 hash of photopoins payload (without upper limits)
			pp_id = hashlib.md5(bytes(pp_hash_payload, "utf-8")).hexdigest()
			

			################
			# Save results #
			################
	
			# Save channel name <-> effective comp id association
			if eff_id in self.d_eid_chnames:
				self.d_eid_chnames[eff_id].add(chan_name)
			else:
				self.d_eid_chnames[eff_id] = {chan_name}

			# Save channel name <-> pp comp id association
			if pp_id in self.d_ppid_chnames:
				self.d_ppid_chnames[pp_id].add(chan_name)
			else:
				self.d_ppid_chnames[pp_id] = {chan_name}
		
			# Save eid <-> ppid association
			self.d_eid_ppid[eff_id] = pp_id

			# Save effective compound
			self.d_eid_comp[eff_id] = eff_comp
	
			# If effective id does not equal strict id, a compound flavor entry 
			# should to be created in the compound document
			if eff_id != strict_id:
	
				self.logger.info(
					"Compound generated for channel %s. CompoundId: (eff: %s, strict: %s)", 
					chan_name, eff_id, strict_id
				)
	
				# Add tupple (chan_name, strict id) to internal dict using eff_id as key
				if not eff_id in self.d_eid_tuple_chan_sid:
					self.d_eid_tuple_chan_sid[eff_id] = {
						(chan_name, strict_id)
					}
				else:
					self.d_eid_tuple_chan_sid[eff_id].add(
						(chan_name, strict_id)
					)

				# Save strict compound using strict id as key
				self.d_sid_compdiff[strict_id] = strict_comp
	
			else:
	
				self.logger.info(
					"Compound generated for channel %s. CompoundId: %s", 
					chan_name, eff_id
				)


	def get_effids_of_chans(self, chan_names):
		"""
		chan_names: list/tuple/set of channel names (string)
		Returns a set of strings containing effective compound ids (string)
		"""
		eids = set()
		for chan_name in chan_names:
			for eid in self.d_eid_chnames.keys():
				if chan_name in self.d_eid_chnames[eid]:
					eids.add(eid)
		return eids
	

	def get_effid_of_chan(self, chan_name):
		"""
		chan_name: channel name (string)
		Returns a set of strings containing effective compound ids (string)
		"""
		for eid in self.d_eid_chnames.keys():
			if chan_name in self.d_eid_chnames[eid]:
				return eid


	def get_chans_with_effid(self, eff_comp_id):
		"""
		Parameter eff_comp_id: effective compound id (string)
		Returns a set of strings containing channel names
		"""
		return self.d_eid_chnames[eff_comp_id]


	def get_eff_compound(self, eff_comp_id):
		"""	
		Parameter eff_comp_id: effective compound id (string)
		Returns the effective compound: a list of dict instances, 
		each created by CompoundElement.gen_dict()
		"""	
		return self.d_eid_comp[eff_comp_id]


	def get_ppids_of_chans(self, chan_names):
		"""
		Parameter chan_names: list/tuple/set of channel names (string)
		Returns a set of strings containing pp compound ids (string)
		"""
		ppids = set()
		for chan_name in chan_names:
			for eid in self.d_ppid_chnames.keys():
				if chan_name in self.d_ppid_chnames[eid]:
					ppids.add(eid)
					break
		return ppids


	def get_ppid_of_effid(self, eff_comp_id):
		"""
		Parameter eff_comp_id: effective compound id (string)
		Returns the associated pp compound id (string)
		"""
		return self.d_eid_ppid[eff_comp_id]


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
		return tuple(
			{'flavor': el[1], 'omitted': self.d_sid_compdiff[el[1]]}
			for el in self.d_eid_tuple_chan_sid[compound_id]
		)


	def get_channel_flavors(self, compound_id):
		"""	
		"""	
		return [
			{
				'channel': [ell.name for ell in el[0].as_list()],
				'flavor': el[1]
			}
			for el in self.d_eid_tuple_chan_sid[compound_id]
		]
