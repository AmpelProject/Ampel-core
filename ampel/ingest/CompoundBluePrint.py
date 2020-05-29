#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/CompoundBluePrint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from dataclasses import dataclass, field
from typing import Set, Optional, Dict, List, Union, Tuple, Any
from ampel.type import ChannelId, StrictIterable, DataPointId
from ampel.content.Compound import CompoundElement


@dataclass
class CompoundBluePrint:
	"""
	Instance members of this class are typically set/updated by T1 units
	Abbreviations used in this class: "eid": effective id, "sid": strict id,
	"chan": channel name, "comp": compound
	"""

	# save compound lists using effective id as key
	d_eid_comp: Dict[bytes, List[Union[DataPointId, CompoundElement]]] = field(default_factory=dict)

	# save channels names using effective id as key
	d_eid_chnames: Dict[bytes, Set[ChannelId]] = field(default_factory=dict)

	# save tuple (chan name, strict id) using effective id as key
	d_eid_tuple_chan_sid: Dict[bytes, Set[Tuple[ChannelId, bytes]]] = field(default_factory=dict)

	# save strict compound difference (wrt to effective compound) using strict if as key
	d_sid_compdiff: Dict[bytes, List[Union[DataPointId, CompoundElement]]] = field(default_factory=dict)

	# save eid <-> compound tags association
	d_eid_comptags: Dict[bytes, Set[str]] = field(default_factory=dict)


	def get_effids_for_chans(self, chan_names: StrictIterable[ChannelId]) -> Set[bytes]:
		"""
		:param chan_names: list/tuple/set of channel names
		:returns: set containing effective compound ids
		"""
		eids = set()
		for chan_name in chan_names:
			for eid in self.d_eid_chnames:
				if chan_name in self.d_eid_chnames[eid]:
					eids.add(eid)
		return eids


	def get_effid_for_chan(self, chan_name: ChannelId) -> Optional[bytes]:
		""" :returns: effective compound id """
		for eid in self.d_eid_chnames:
			if chan_name in self.d_eid_chnames[eid]:
				return eid
		return None


	def get_chans_with_effid(self, eff_comp_id: bytes) -> Set[ChannelId]:
		"""
		:param eff_comp_id: effective compound id
		:returns: a set containing channel ids
		"""
		return self.d_eid_chnames[eff_comp_id]


	def get_comp_tags(self, eff_comp_id: bytes) -> Set[str]:
		"""
		:param eff_comp_id: effective compound id
		"""
		return self.d_eid_comptags[eff_comp_id]


	def get_eff_compound(self, eff_comp_id: bytes) -> List[Union[DataPointId, CompoundElement]]:
		return self.d_eid_comp[eff_comp_id]


	def has_flavors(self, compound_id: bytes) -> bool:

		if compound_id not in self.d_eid_tuple_chan_sid:
			return False

		if len(self.d_eid_tuple_chan_sid[compound_id]) == 0:
			return False

		return True


	def get_compound_flavors(self, compound_id: bytes) -> List[Dict[str, Any]]:

		return [
			{'flavor': el[1], 'omitted': self.d_sid_compdiff[el[1]]}
			for el in self.d_eid_tuple_chan_sid[compound_id]
		]


	def get_channel_flavors(self, compound_id: bytes) -> List[Dict[str, Any]]:

		return [
			{'channel': el[0], 'flavor': el[1]}
			for el in self.d_eid_tuple_chan_sid[compound_id]
		]
