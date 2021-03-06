#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/select/T3StockSelector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.12.2019
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo.cursor import Cursor
from typing import Union, Optional, Dict, Literal

from ampel.type import ChannelId, Tag
from ampel.db.query.stock import build_stock_query
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
from ampel.log.utils import safe_query_dict
from ampel.t3.select.AbsT3Selector import AbsT3Selector
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.time.TimeConstraintModel import TimeConstraintModel
from ampel.config.LogicSchemaUtils import LogicSchemaUtils


class T3StockSelector(AbsT3Selector):
	"""
	Default stock selector used by T3Processor
	
	Example configuration::
	  
	  {
	     "created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
	     "modified": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
	     "channel": "HU_GP_CLEAN",
	     "tags": {"with": "ZTF", "without": "HAS_ERROR"}
	  }
	"""

	logger: AmpelLogger
	#: Select by creation time
	created: Optional[TimeConstraintModel] = None
	#: Select by modification time
	modified: Optional[TimeConstraintModel] = None
	#: Select by channel
	channel: Optional[Union[ChannelId, AnyOf[ChannelId], AllOf[ChannelId], OneOf[ChannelId]]] = None
	#: Select by tag
	tag: Optional[Dict[Literal['with', 'without'], Union[Tag, Dict, AllOf[Tag], AnyOf[Tag], OneOf[Tag]]]] = None


	def __init__(self, **kwargs):

		if 'channel' in kwargs:
			kwargs['channel'] = LogicSchemaUtils.to_logical_struct(kwargs['channel'], 'channel')

		if 'tag' in kwargs:
			kwargs['tag'] = {
				k: LogicSchemaUtils.to_logical_struct(v, 'tag')
				for k,v in kwargs['tag'].items()
			}

		super().__init__(**kwargs)
		if self.logger is None:
			raise ValueError("Parameter logger cannot be None")


	# Override/Implement
	def fetch(self) -> Optional[Cursor]:

		# Build query for matching transients using criteria defined in config
		match_query = build_stock_query(
			channel = self.channel,
			tag = self.tag,
			time_created = self.created.get_query_model(db=self.context.db) \
				if self.created else None,
			time_modified = self.modified.get_query_model(db=self.context.db) \
				if self.modified else None,
		)

		if self.logger.verbose:
			self.logger.log(VERBOSE, "Executing search query", extra=safe_query_dict(match_query))

		# Execute 'find transients' query
		cursor = self.context.db \
			.get_collection('stock') \
			.find(
				match_query,
				{'_id': 1}, # indexed query
				no_cursor_timeout = True, # allow query to live for > 10 minutes
			) \
			.hint('_id_1_channel_1')

		# Count results
		if cursor.count() == 0:
			self.logger.info("No transient matches the given criteria")
			return None

		self.logger.info(
			f"{cursor.count()} transients match search criteria"
		)

		return cursor
