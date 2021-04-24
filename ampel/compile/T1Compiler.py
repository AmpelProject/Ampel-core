#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/compile/T1Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 24.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Set, Optional, Dict, List, Union, Tuple, Any, Sequence, Callable
from ampel.type import ChannelId, StrictIterable, DataPointId, Tag
from ampel.content.T1Record import T1Record
from ampel.log.AmpelLogger import AmpelLogger


class T1Compiler:
	"""
	Helps build a minimal set of :class:`compounds <ampel.content.T1Document.T1Document>`
	that represent a collection of :class:`datapoints <ampel.content.DataPoint.DataPoint>`,
	as viewed through a set of channels.

	Different channels may select different subsets of datapoints associated with a stock.
	In addition, some datapoints may be part of a channel's selection, but
	explicitly excluded by a policy, for example one that requires significant
	detections above the noise level.

	This leads to two different identifiers for a subselection:

	strict id:
	  The hash of all the datapoints the subselection contains
	effective id:
	  The hash of only the datapoints that were not marked excluded

	Only one :class:`~ampel.content.T1Document.T1Document` will be created for each
	*effective* subselection. This allows downstream calculations that operate on equivalent
	subselections to be performed only once. The variants corresponding to each distinct
	strict id may be included as metadata in subclasses of
	:class:`~ampel.content.T1Document.T1Document`.
	"""

	# save compound lists using effective id as key
	#: Mapping from effective id to compound contents
	eid_payload: Dict[bytes, Sequence[Union[DataPointId, T1Record]]]

	# save channels names using effective id as key
	#: Mapping from effective id to channels
	eid_chans: Dict[bytes, Set[ChannelId]]

	# save tuple (chan name, strict id) using effective id as key
	#: Mapping from effective id to (channel, strict id)
	eid_chan_sid: Optional[Dict[bytes, Set[Tuple[ChannelId, bytes]]]]

	# save eid <-> compound tags association
	#: Mapping from effective id to compound tags
	eid_tags: Dict[bytes, Set[Tag]]

	# save strict compound difference (wrt to effective compound) using strict if as key
	#: Mapping from strict id to the difference of the strict and effective
	#: compounds, i.e. the set of points excluded from the effective compound
	sid_elem_diff: Optional[Dict[bytes, Sequence[Union[DataPointId, T1Record]]]]


	def __init__(self,
		t1_res: Dict[ChannelId, Tuple[Set[Tag], Sequence[Union[DataPointId, T1Record]]]],
		inner_hash: Callable, outer_hash: Callable, sort: bool = True, logger: Optional[AmpelLogger] = None
	):

		self.eid_payload = {}
		self.eid_chans = {}
		self.eid_tags = {}
		self.eid_chan_sid = None
		self.sid_elem_diff = None

		# Cache (python's index() is fast). Cannot use dict (non hashable objs)
		list_eff_dps: List[Any] = []
		list_eff_ids: List[bytes] = []

		# Main loop through provided channels
		for chan_name, (tags, dps) in t1_res.items():

			eff_dps = [
				el for el in dps
				if not (isinstance(el, dict) and 'excl' in el)
			]

			if not eff_dps:
				if logger:
					logger.error(f"Empty compound for channel {chan_name}")
				continue

			try:
				# fast index() check raises ValueError if not found
				i = list_eff_dps.index(eff_dps)
				eff_id = list_eff_ids[i]
			except Exception:

				eff_payload = [
					el if isinstance(el, int) else
					int.from_bytes(
						el if isinstance(el, bytes)
						else inner_hash(
							(el if isinstance(el, str) else repr(el)).encode()
						).digest(),
						"little"
					)
					for el in eff_dps
				]

				eff_id = outer_hash(
					repr(sorted(eff_payload) if sort else eff_payload).encode()
				).digest()

				# Update cache (avoids un-necessary digests)
				list_eff_dps.append(eff_dps)
				list_eff_ids.append(eff_id)

			# Subclasses might need to compute additional values
			# self.combine_extra(compiler, chan_name, eff_id, dps)

			# Save channel name <-> effective comp id association
			if eff_id in self.eid_chans:
				self.eid_chans[eff_id].add(chan_name)
				self.eid_tags[eff_id] |= tags
			else:
				self.eid_chans[eff_id] = {chan_name}
				self.eid_tags[eff_id] = tags

			# Save effective compound
			self.eid_payload[eff_id] = eff_dps

			# If effective payload does not equal strict payload,
			# a compound flavor entry should to be created in the compound document
			if len(eff_dps) != len(dps):

				if not self.sid_elem_diff:
					self.sid_elem_diff = {}

				if not self.eid_chan_sid:
					self.eid_chan_sid = {}

				strict_payload = [
					el if isinstance(el, int) else
					int.from_bytes(
						el if isinstance(el, bytes)
						else inner_hash(
							(el if isinstance(el, str) else repr(el)).encode()
						).digest(),
						"little"
					)
					for el in dps
				]

				strict_id = outer_hash(
					repr(sorted(strict_payload) if sort else strict_payload).encode()
				).digest()

				if logger and logger.verbose > 1:
					logger.debug(
						None, extra={'channel': chan_name, 'eff': eff_id, 'strict': strict_id}
					)

				# Add tupple (chan_name, strict id) to internal dict using eff_id as key
				if eff_id not in self.eid_chan_sid:
					self.eid_chan_sid[eff_id] = {(chan_name, strict_id)}
				else:
					self.eid_chan_sid[eff_id].add((chan_name, strict_id))

				# Save strict compound using strict id as key
				self.sid_elem_diff[strict_id] = dps


	def get_effids_for_chans(self, chan_names: StrictIterable[ChannelId]) -> Set[bytes]:
		"""
		:param chan_names: list/tuple/set of channel names
		:returns: a set of effective compound ids representing the union of all
		  channels' views into the underlying datapoints
		"""
		eids = set()
		for chan_name in chan_names:
			for eid in self.eid_chans:
				if chan_name in self.eid_chans[eid]:
					eids.add(eid)
		return eids


	def get_effid_for_chan(self, chan_name: ChannelId) -> Optional[bytes]:
		"""
		:param chan: channel name
		:returns: the effective compound id for the channel, or None if no
		  datapoints were selected for the channel
		"""
		for eid in self.eid_chans:
			if chan_name in self.eid_chans[eid]:
				return eid
		return None


	def get_chans_with_effid(self, eff_id: bytes) -> Set[ChannelId]:
		"""
		:param eff_id: effective compound id
		:returns: the set of channels with the same effective compound id
		"""
		return self.eid_chans[eff_id]


	def get_doc_tags(self, eff_id: bytes) -> Set[Tag]:
		"""
		:param eff_id: effective compound id
		:returns: tags for the given compound
		"""
		return self.eid_tags[eff_id]


	def get_eff_compound(self, eff_id: bytes) -> Sequence[Union[DataPointId, T1Record]]:
		"""
		:param eff_id: effective compound id
		:returns: compound contents
		"""
		return self.eid_payload[eff_id]


	def has_flavors(self, compound_id: bytes) -> bool:
		"""
		:returns: True if the effective id corresponds to more than one strict id
		"""

		if not self.eid_chan_sid or compound_id not in self.eid_chan_sid:
			return False

		if len(self.eid_chan_sid[compound_id]) == 0:
			return False

		return True


	def get_compound_flavors(self, compound_id: bytes) -> List[Dict[str, Any]]:
		"""
		:param compound_id: effective compound id
		:returns: the "flavors", or strict ids that correspond to the effective
		  id, and the set of points excluded from each
		"""

		if not self.eid_chan_sid or not self.sid_elem_diff:
			return []

		return [
			{'flavor': el[1], 'omitted': self.sid_elem_diff[el[1]]}
			for el in self.eid_chan_sid[compound_id]
		]


	def get_channel_flavors(self, compound_id: bytes) -> List[Dict[str, Any]]:
		"""
		:param compound_id: effective compound id
		:returns: the "flavors", or strict ids that correspond to the effective
		  id, and channel each was created for
		"""

		if not self.eid_chan_sid:
			return []

		return [
			{'channel': el[0], 'flavor': el[1]}
			for el in self.eid_chan_sid[compound_id]
		]
