#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/var/LogsMatcher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.11.2018
# Last Modified Date: 26.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections.abc
from typing import Dict, Any, Union, Sequence, Optional, get_args
from datetime import datetime
from bson.objectid import ObjectId
from ampel.types import ChannelId, StockId
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.util.collections import check_seq_inner_type
from ampel.mongo.schema import apply_schema
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf


class LogsMatcher:


	@classmethod
	def new(cls,
		after: Optional[str] = None,
		before: Optional[str] = None,
		channel: Optional[Union[ChannelId, Sequence[ChannelId]]] = None,
		stock: Optional[Union[StockId, Sequence[StockId]]] = None,
		run: Optional[Union[int, Sequence[int], Dict[str, Any]]] = None,
		custom: Optional[Dict[str, Any]] = None,
		id_mapper: Optional[AbsIdMapper] = None,
		flag: int = None,
		**kwargs # ignored, added for cli convenience
	) -> "LogsMatcher":

		matcher = cls()

		if after:
			matcher.set_after(after)

		if before:
			matcher.set_before(before)

		if channel:
			matcher.set_channel(channel)

		if id_mapper:
			matcher.set_id_mapper(id_mapper)

		if stock:
			matcher.set_stock(stock)

		if run:
			matcher.set_run(run)

		if custom:
			matcher.set_custom(
				next(iter(custom.keys())),
				next(iter(custom.values()))
			)

		if flag:
			matcher.set_flag(flag)

		return matcher


	def __init__(self):
		self.match: Dict[str, Any] = {}
		self.id_mapper = None


	def get_match_criteria(self) -> Dict[str, Any]:
		return self.match


	def set_id_mapper(self, id_mapper: AbsIdMapper) -> None:
		self.id_mapper = id_mapper


	def set_flag(self, arg) -> 'LogsMatcher':
		""" Mongo's operator "$bitsAllSet" is used """
		self.match['f'] = {"$bitsAllSet": arg}
		return self


	def set_channel(self,
		channel: Union[ChannelId, Sequence[ChannelId], AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]],
		compact_logs: bool = True
	) -> 'LogsMatcher':


		if isinstance(channel, get_args(ChannelId)):
			v: Any = channel
		elif isinstance(channel, (dict, AllOf, AnyOf, OneOf)):
			v = apply_schema(self.match, 'channel', channel)
		elif isinstance(channel, collections.abc.Sequence):
			v = {'$in': channel}
		else:
			raise ValueError(f"Unsupported type for parameter channel: {type(channel)}")


		if compact_logs:
			self.match['$or'] = [{'c': v}, {'m.c': v}]
		else:
			self.match['c'] = v

		return self


	def set_stock(self,
		stock_id: Union[StockId, Sequence[StockId]]
	) -> 'LogsMatcher':
		if (
			self.id_mapper and (
				isinstance(stock_id, str) or
				check_seq_inner_type(stock_id, str)
			)
		):
			self.match['s'] = self.id_mapper.to_ampel_id(stock_id) \
				if isinstance(stock_id, str) else {'$in': self.id_mapper.to_ampel_id(stock_id)}
		else:
			self.match['s'] = stock_id if isinstance(stock_id, get_args(StockId)) else {'$in': stock_id}
		return self


	def set_run(self, run_id: Union[int, Sequence[int], Dict[str, Any]]) -> 'LogsMatcher':
		self.match['r'] = run_id if isinstance(run_id, (int, dict)) else {'$in': run_id}
		return self


	def set_custom(self, key: str, value: Any) -> 'LogsMatcher':
		if (isinstance(value, collections.abc.Sequence) and not isinstance(value, str)):
			self.match[key] = {'$in': value}
		else:
			self.match[key] = value
		return self


	def set_after(self, dt: Union[datetime, str]) -> 'LogsMatcher':
		"""
		Note: time operation is greater than / *equals*
		:param dt: either datetime object or string (datetime.fromisoformat is used)
		"""
		return self._set_time_constraint(dt, '$gte')


	def set_before(self, dt: Union[datetime, str]) -> 'LogsMatcher':
		"""
		Note: time operation is before than / *equals*
		:param dt: either datetime object or string (datetime.fromisoformat is used)
		"""
		return self._set_time_constraint(dt, '$lte')


	def _set_time_constraint(self, dt: Union[datetime, str], op: str) -> 'LogsMatcher':
		"""
		Note: time operation is greater than / *equals*
		:param dt: either datetime object or string (datetime.fromisoformat is used)
		"""

		if isinstance(dt, datetime):
			pass
		elif isinstance(dt, str):
			dt = datetime.fromisoformat(dt)
		else:
			raise ValueError()

		if "_id" not in self.match:
			self.match["_id"] = {}

		self.match["_id"][op] = ObjectId.from_datetime(dt)

		return self
