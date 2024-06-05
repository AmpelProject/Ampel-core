#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t2/T2Utils.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.03.2021
# Last Modified Date:  16.09.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any, Literal

from pymongo.collection import Collection

from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.content.JournalRecord import JournalRecord
from ampel.content.T2Document import T2Document
from ampel.enum.DocumentCode import DocumentCode
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.mongo.query.general import build_general_query
from ampel.mongo.utils import maybe_match_array
from ampel.types import ChannelId, StockId, StrictIterable, Tag, UnitId
from ampel.util.collections import check_seq_inner_type


class T2Utils:


	def __init__(self, logger: AmpelLogger):
		self._confirm = False
		self.logger = logger


	def i_know_what_i_am_doing(self) -> "T2Utils":
		self._confirm = True
		return self


	def reset_t2s(self, col: Collection, run_id: int, soft: bool = False, cli: bool = False, **kwargs) -> int:
		"""
		:param soft: if True, 'body' will not be deleted
		"""

		if not self._confirm:
			self.logger.warn("Danger zone: please confirm you know what you are doing")
			return 0

		jrec = JournalRecord(tier=-1, run=run_id, ts=datetime.now(tz=timezone.utc).timestamp())

		if cli:
			jrec['extra'] = {'cli': True}

		update: dict[str, Any] = {
			"$set": {"code": DocumentCode.NEW},
			"$push": {"journal": jrec}
		}

		if not soft:
			# remove body
			update["$unset"] = {"body": 1}
			# remove records of previous execution attempts
			update["$pull"] = {"meta": {"tier": 2}}

		return col \
			.update_many(self.match_t2s(**kwargs), update) \
			.modified_count


	def get_t2s(self, col: Collection, **kwargs) -> Iterable[T2Document]:
		return col.find(
			self.match_t2s(**kwargs)
		)


	def match_t2s(self,
		unit: None | UnitId | StrictIterable[UnitId] = None,
		config: None | str | int = None,
		code: None | int | Sequence[int] = None,
		link: None | str | Sequence[str] = None,
		stock: None | StockId | StrictIterable[StockId] = None,
		channel: None | ChannelId | dict | AllOf[ChannelId] | AnyOf[ChannelId] | OneOf[ChannelId] = None,
		tag: None | dict[Literal['with', 'without'], Tag | dict | AllOf[Tag] | AnyOf[Tag] | OneOf[Tag]] = None,
		custom: None | dict[str, Any] = None,
		id_mapper: None | AbsIdMapper = None,
		**kwargs
	) -> dict[str, Any]:
		"""
		:param config: use string "null" to match null
		:param link: hex encoded bytes
		:param custom: custom match criteria, for example: {"body.result.sncosmo_info.chisqdof": {'$lte': 4}}
		"""

		if id_mapper and (isinstance(stock, str) or check_seq_inner_type(stock, str)):
			stock = id_mapper.to_ampel_id(stock) # type: ignore[arg-type]

		match = build_general_query(stock=stock, channel=channel, tag=tag)

		if unit:
			if isinstance(unit, int | str):
				match['unit'] = unit
			elif isinstance(unit, tuple | list):
				match['unit'] = unit[0] if len(unit) == 1 else {'$in': unit}
			else:
				raise ValueError(f"Unrecognized 'unit' argument type: {type(unit)}")

		if config:
			if isinstance(config, str) and config == "null":
				match['config'] = None
			elif isinstance(config, int):
				match['config'] = config
			elif isinstance(config, list | tuple):
				match['config'] = {'$in': [el for el in config if config != "null"]}
				if "null" in config:
					match['config']['$in'].append(None)

		if link:
			match['link'] = bytes.fromhex(link) if isinstance(link, str) else {'$in': [bytes.fromhex(el) for el in link]}

		if custom:
			match.update(custom)

		if code is not None:
			match['code'] = code if isinstance(code, int) else maybe_match_array(list(code))

		if kwargs.get('debug'):
			self.logger.debug(f"Using following matching criteria: {match}")

		return match
