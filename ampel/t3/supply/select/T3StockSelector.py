#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/supply/select/T3StockSelector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.12.2019
# Last Modified Date: 03.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo.cursor import Cursor
from typing import Union, Optional, Dict, Literal, Any

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
	created: Optional[TimeConstraintModel] = None

	#: Select by modification time
	updated: Optional[TimeConstraintModel] = None

	#: Select by channel
	channel: Optional[Union[ChannelId, AnyOf[ChannelId], AllOf[ChannelId], OneOf[ChannelId]]] = None

	#: Select by tag
	tag: Optional[
		Dict[
			Literal['with', 'without'],
			Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]
		]
	] = None

	#: Custom selection (ex: {'run': {'$gt': 10}})
	custom: Optional[Dict[str, Any]] = None


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
	def fetch(self) -> Optional[Cursor]:

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

		if self.logger.verbose:
			self.logger.log(VERBOSE, "Executing search query", extra=safe_query_dict(match_query))

		# Execute 'find transients' query
		cursor = self.context.db \
			.get_collection('stock') \
			.find(match_query, {'stock': 1})

		return cursor
