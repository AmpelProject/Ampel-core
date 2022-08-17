#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/supply/select/T3StockSelector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                06.12.2019
# Last Modified Date:  16.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from pymongo.cursor import Cursor
from typing import Literal, Any
from ampel.types import ChannelId, Tag
from ampel.mongo.query.stock import build_stock_query
from ampel.util.logicschema import to_logical_dict
from ampel.log.utils import safe_query_dict
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
from ampel.abstract.AbsT3Selector import AbsT3Selector
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.time.TimeConstraintModel import TimeConstraintModel
from ampel.util.collections import try_reduce


class T3StockSelector(AbsT3Selector):
	"""
	Default stock selector used by T3Processor
	
	Example configuration::
	  
	  {
	     "created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
	     "updated": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
	     "channel": "HU_GP_CLEAN",
	     "tags": {"with": "ZTF", "without": "HAS_ERROR"}
	  }
	"""

	#: Select by creation time
	created: None | TimeConstraintModel = None

	#: Select by modification time
	updated: None | TimeConstraintModel = None

	#: Select by channel
	channel: None | ChannelId | AnyOf[ChannelId] | AllOf[ChannelId] | OneOf[ChannelId] = None

	#: Select by tag
	tag: None | dict[Literal['with', 'without'], Tag | AllOf[Tag] | AnyOf[Tag] | OneOf[Tag] | dict] = None

	#: Custom selection (ex: {'run': {'$gt': 10}})
	custom: None | dict[str, Any] = None


	def __init__(self, logger: AmpelLogger, **kwargs):

		if 'channel' in kwargs:
			kwargs['channel'] = to_logical_dict(kwargs['channel'], 'channel')

		if 'tag' in kwargs:
			kwargs['tag'] = {
				k: to_logical_dict(v, 'tag')
				for k, v in kwargs['tag'].items()
			}

		if logger is None:
			raise ValueError("Parameter logger cannot be None")

		self.logger = logger
		super().__init__(**kwargs)


	# Override/Implement
	def fetch(self) -> None | Cursor:

		# Build query for matching transients using criteria defined in config
		match_query = build_stock_query(
			channel = self.channel,
			tag = self.tag,
			time_created = self.created.get_query_model(db=self.context.db)
				if self.created else None,
			time_updated = self.updated.get_query_model(db=self.context.db)
				if self.updated else None,
		)

		if self.custom:
			match_query.update(self.custom)

		col = self.context.db.get_collection('stock')

		if self.logger.verbose:
			self.logger.log(
				VERBOSE,
				f"Executing stock search query [{col.database.name} "
				f"{try_reduce(list(col.database.client.nodes))}]",
				extra=safe_query_dict(match_query)
			)

		# Execute 'find transients' query
		return col.find(match_query, {'stock': 1})
