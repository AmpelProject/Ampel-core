#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T1DefaultCombiner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json
from typing import Sequence, List, Union, Generic, TypeVar, Tuple, Optional, Set, Iterable
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import CompoundElement
from ampel.ingest.CompoundBluePrint import CompoundBluePrint
from ampel.type import StockId, ChannelId, DataPointId
from ampel.abstract.AbsT1Unit import AbsT1Unit
from ampel.log import VERBOSE

T = TypeVar("T", bound=CompoundBluePrint)

class T1DefaultCombiner(Generic[T], AbsT1Unit[T]):
	"""
	Purpose of this class is to generate a "CompoundBluePrint",
	which is a dataclass containing all information required to
	generate a compound document and the t2 documents associated with it.
	Known subclass: PhotoCompoundBuilder from distribution "ampel-photometry".
	Used abbreviations: "eid": effective id, "sid": strict id,
	"chan": channel name, "comp": compound
	"""

	debug: bool = False

	def combine(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		channel_names: Iterable[ChannelId]
	) -> T:
		"""
		:param datapoints: sequence of dict instances representing photopoint or upperlimis measurements
		!IMPORTANT!: sequence must be timely sorted.
		Sorting is not performed within this method because CompoundBluePrint aims at being instrument independant.
		(For ZTF, the field 'jd' is used: -> sorted(datapoints, key=lambda k: k['jd']))
		"""

		cbp = self.BluePrintClass()
		gen_comp_sub_entry = self.gen_sub_entry # shortcut
		if not isinstance(stock_id, str):
			stock_id_str = str(stock_id)
		else:
			stock_id_str = stock_id

		# Main loop through provided channels
		for chan_name in channel_names:

			# Generate compounds, md5s, and flavors
			#######################################

			# Effective compound contains only non-excluded datapoints
			eff_comp: List[Union[DataPointId, CompoundElement]] = []
			strict_comp: List[Union[DataPointId, CompoundElement]] = []
			eff_hash_payload: List[str] = [stock_id_str]
			strict_hash_payload = stock_id_str
			tags: Set[Union[int, str]] = set()

			# Create compound and compoundId
			for dp in datapoints:

				# Generate compound entry dictionary
				comp_entry, t = gen_comp_sub_entry(dp, chan_name)

				if t:
					tags |= t

				# Append generated dict to effective compound list
				strict_comp.append(comp_entry)

				# Check if policy or exclusion
				# In both case, strict id payload must be appended
				if isinstance(comp_entry, dict):

					# Build str repr of dict. sort_keys is important !
					dict_str = json.dumps(comp_entry, sort_keys=True)

					# Append dict string to strict payload
					strict_hash_payload += dict_str

					# Photopoint or upper limit has a custom policy
					if 'excl' not in comp_entry:

						# For this class, a datapoint defined with policy is just like
						# a different datapoint (meaning a datapoint with a different ID).
						# A datapoint might for example return a different magnitude
						# when associated with a policy.
						# Datapoint with policies will result in a different effective payload
						# and thus a different effective id
						eff_hash_payload.append(dict_str)
						eff_comp.append(comp_entry)

					else:
						if self.debug:
							self.logger.debug("Excluded datapoint", extra=comp_entry) # type: ignore

				# No policy or exclusion
				else:

					# pps or uls ids are long (ZTF-IPAC)
					id_str = str(comp_entry)

					# Append string repr of dict to effective and strict payloads
					eff_comp.append(comp_entry)
					eff_hash_payload.append(id_str)
					strict_hash_payload += id_str

			if not eff_comp:
				self.logger.error(f"Empty compound for channel {chan_name}")
				continue

			# eff_id = effective compound id = md5 hash of effective payload
			# Note: ho is the abbreviation of "hash object"
			eff_id: bytes = hashlib \
				.md5(bytes("".join(eff_hash_payload), "utf-8")) \
				.digest()

			# strict_id = strict compound id = md5 hash of strict payload
			strict_id: bytes = hashlib \
				.md5(bytes(strict_hash_payload, "utf-8")) \
				.digest()

			# Subclasses might need to compute additional values
			self.combine_extra(cbp, chan_name, eff_id, eff_comp, eff_hash_payload)

			# Save results
			##############

			# Save channel name <-> effective comp id association
			if eff_id in cbp.d_eid_chnames:
				cbp.d_eid_chnames[eff_id].add(chan_name)
				cbp.d_eid_comptags[eff_id] |= tags
			else:
				cbp.d_eid_chnames[eff_id] = {chan_name}
				cbp.d_eid_comptags[eff_id] = tags

			# Save effective compound
			cbp.d_eid_comp[eff_id] = eff_comp

			# If effective id does not equal strict id, a compound flavor entry
			# should to be created in the compound document
			if eff_id != strict_id:
				if self.logger:
					self.logger.log(VERBOSE,
						None, extra = {
							'channel': chan_name,
							'eff': eff_id,
							'strict': strict_id
						}
					)

				# Add tupple (chan_name, strict id) to internal dict using eff_id as key
				if eff_id not in cbp.d_eid_tuple_chan_sid:
					cbp.d_eid_tuple_chan_sid[eff_id] = {(chan_name, strict_id)}
				else:
					cbp.d_eid_tuple_chan_sid[eff_id].add((chan_name, strict_id))

				# Save strict compound using strict id as key
				cbp.d_sid_compdiff[strict_id] = strict_comp

		# Return instance of CompoundBluePrint
		return cbp if cbp.d_eid_comp else None


	def gen_sub_entry(self,
		dp: DataPoint, channel_name: ChannelId
	) -> Tuple[Union[DataPointId, CompoundElement], Optional[Set[Union[int, str]]]]:
		"""
		Method can be overriden by subclasses.
		Known overriding class: ZiCompoundBuilder (distrib ampel-ZTF)
		"""

		# Channel specific exclusion. dp["excl"] could look like this: ["HU_SN", "HU_GRB"]
		if "excl" in dp and channel_name in dp['excl']: # type: ignore
				return {'id': dp['_id'], 'excl': 'Manual'}, {'HAS_EXCLUDED_PPS', 'MANUAL_EXCLUSION'}

		return dp['_id'], None


	def combine_extra(self,
		blue_print: T, chan_name: ChannelId, eff_id: bytes,
		eff_comp: List[Union[DataPointId, CompoundElement]],
		eff_hash_payload: List[str]
	) -> None:
		"""
		Method can be overriden by subclasses.
		Known overriding class: T1PhotoCompoundBuilder (distrib ampel-photometry)
		"""
		pass
