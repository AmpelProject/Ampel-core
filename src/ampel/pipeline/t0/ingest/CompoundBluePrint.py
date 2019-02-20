#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/CompoundBluePrint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json

class CompoundBluePrint:
	"""
	Instance members of this class are typically set/updated by CompoundBluePrintGenerator
	Name shortcuts used in this class:
	"eid": effective id
	"sid": strict id
	"chan": channel name
	"flagpos": index position of flag
	"comp": compound
	"opt": option
	"""

	def __init__(self):
		"""
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

		#7 save eid <-> compound tags association
		self.d_eid_comptags = {}


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


	def get_comp_tags(self, eff_comp_id):
		"""	
		Parameter eff_comp_id: effective compound id (string)
		"""	
		return self.d_eid_comptags[eff_comp_id]


	def get_eff_compound(self, eff_comp_id):
		"""	
		Parameter eff_comp_id: effective compound id (string)
		Returns the effective compound: a list of dict instances, 
		each created by CompoundShaper.gen_compound_item()
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
