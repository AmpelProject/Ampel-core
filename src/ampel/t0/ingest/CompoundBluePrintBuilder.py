#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/ingest/CompoundBluePrintBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 19.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json
from bson import Binary
from typing import Union, Any, Sequence, Dict
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.t0.ingest.CompoundBluePrint import CompoundBluePrint
from ampel.abstract.AbsCompoundItemBuilder import AbsCompoundItemBuilder

class CompoundBluePrintBuilder:
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

	def __init__(
		self, comp_itm_builder: AbsCompoundItemBuilder, logger: AmpelLogger, verbose: bool = False
	):
		"""
		:param comp_itm_builder: child *class* (not instance) of ampel.abstract.AbsCompoundItemGenerator
		:param logger: AmpelLogger instance
		:returns: None
		"""

		self.logger = logger
		self.verbose = verbose
		self.comp_itm_builder = comp_itm_builder


	def build(
		self, tran_id: Union[int, str], dicts: Sequence[Dict[str, Any]], channel_names: Sequence[str]
	) -> CompoundBluePrint:
		"""	
		:param tran_id: transient id (int or str)
		:param dicts: sequence of dict instances representing photopoint or upperlimis measurements
		######################################
		IMPORTANT: sequence must be timely sorted.
		######################################
		The sorting is not done within this method because CompoundBluePrint 
		aims at being instrument independant.  For ZTF, the field 'jd' is used:
		-> sorted(dicts, key=lambda k: k['jd'])
		:param channel_names: iterable sequence of channel names (int or string)
		:returns: instance of CompoundBluePrint
		"""	

		cbp = CompoundBluePrint()
		tran_id_str = str(tran_id)

		# Main loop through provided channels
		for chan_name in channel_names:

			#########################################
			# Generate compounds, md5s, and flavors #
			#########################################
	
			eff_comp = []
			strict_comp = []
			eff_hash_payload = strict_hash_payload = pp_hash_payload = tran_id_str
	
			# Create compound and compoundId
			for el in dicts:
	
				# Generate compound entry dictionary
				comp_entry, tags = self.comp_itm_builder.build(el, chan_name)	

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
			eff_id_ho = hashlib.md5(bytes(eff_hash_payload, "utf-8"))
			eff_id = eff_id_ho.digest()

			# strict_id = strict compound id = md5 hash of strict payload
			strict_id_ho = hashlib.md5(bytes(strict_hash_payload, "utf-8"))
			strict_id = strict_id_ho.digest()
	
			# pp_id = photopoints compound id = md5 hash of photopoins payload (without upper limits)
			pp_id_ho = hashlib.md5(bytes(pp_hash_payload, "utf-8"))
			pp_id = pp_id_ho.digest()
			

			################
			# Save results #
			################
	
			# Save channel name <-> effective comp id association
			if eff_id in cbp.d_eid_chnames:
				cbp.d_eid_chnames[eff_id].add(chan_name)
				cbp.d_eid_comptags[eff_id] |= tags
			else:
				cbp.d_eid_chnames[eff_id] = {chan_name}
				cbp.d_eid_comptags[eff_id] = tags

			# Save channel name <-> pp comp id association
			if pp_id in cbp.d_ppid_chnames:
				cbp.d_ppid_chnames[pp_id].add(chan_name)
			else:
				cbp.d_ppid_chnames[pp_id] = {chan_name}
		
			# Save eid <-> ppid association
			cbp.d_eid_ppid[eff_id] = pp_id

			# Save effective compound
			cbp.d_eid_comp[eff_id] = eff_comp
	
			# If effective id does not equal strict id, a compound flavor entry 
			# should to be created in the compound document
			if eff_id != strict_id:
	
				if self.verbose:
					self.logger.info(
						None, extra={
							'channels': chan_name,
							'compIdEff': Binary(eff_id, 0),
							'compIdStrict': Binary(strict_id, 0)
						}
					)
	
				# Add tupple (chan_name, strict id) to internal dict using eff_id as key
				if eff_id not in cbp.d_eid_tuple_chan_sid:
					cbp.d_eid_tuple_chan_sid[eff_id] = {
						(chan_name, strict_id)
					}
				else:
					cbp.d_eid_tuple_chan_sid[eff_id].add(
						(chan_name, strict_id)
					)

				# Save strict compound using strict id as key
				cbp.d_sid_compdiff[strict_id] = strict_comp
	
		# Return instance of CompoundBluePrint
		return cbp
