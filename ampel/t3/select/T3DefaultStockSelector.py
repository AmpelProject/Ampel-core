#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t3/select/T3DefaultStockSelector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.12.2019
# Last Modified Date: 26.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional
from pydantic import validator
from pymongo.cursor import Cursor

from ampel.query.QueryMatchStock import QueryMatchStock
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.abstract.AbsStockSelector import AbsStockSelector
from ampel.utils.docstringutils import gendocstring
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.time.TimeConstraintModel import TimeConstraintModel
from ampel.config.LogicSchemaUtils import LogicSchemaUtils
from ampel.config.AmpelConfig import AmpelConfig


class T3DefaultStockSelector(AbsStockSelector):
	"""
	Default stock/transient selector used by T3Processor
	"""

	@gendocstring
	class RunConfig(AmpelBaseModel):
		"""
		Example:
		.. sourcecode:: python\n
		{
			"select": {
				"created": {"after": {"use": "$timeDelta", "arguments": {"days": -40}}},
				"modified": {"after": {"use": "$timeDelta", "arguments": {"days": -1}}},
				"channels": "HU_GP_CLEAN",
				"withTags": "SURVEY_ZTF",
				"withoutTags": "HAS_ERROR"
			}
		}
		"""

		created: Optional[TimeConstraintModel] = None
		modified: Optional[TimeConstraintModel] = None
		channels: Optional[Union[AnyOf, AllOf, OneOf]] = None
		withTags: Optional[Union[AnyOf, AllOf, OneOf]] = None
		withoutTags: Optional[Union[AnyOf, AllOf, OneOf]] = None

		# pylint: disable=unused-argument,no-self-argument,no-self-use
		@validator('channels', 'withTags', 'withoutTags', pre=True, whole=True)
		def cast(cls, v, values, **kwargs):
			""" """
			return LogicSchemaUtils.to_logical_struct(
				v, kwargs['field'].name.split("_")[0]
			)

	# pylint: disable=super-init-not-called
	def __init__(self,
		ampel_config: AmpelConfig, init_config=None,
		options=None, logger=None
	):
		self._ampel_db = ampel_config.get_db()


	@staticmethod
	def get_field_name() -> str:
		return "_id"


	# Override/Implement
	def get(self, run_config: RunConfig, logger: AmpelLogger) -> Cursor:
		"""
		Note: the returned Iterable (see type hint of the abstract class) is a pymongo Cursor
		"""

		# Build query for matching transients using criteria defined in config
		match_query = QueryMatchStock.build_query(
			channels = run_config.channels,
			time_created = run_config.created.get_query_model(db=self._ampel_db) \
				if run_config.created else None,
			time_modified = run_config.modified.get_query_model(db=self._ampel_db) \
				if run_config.modified else None,
			with_tags = run_config.withTags,
			without_tags = run_config.withoutTags
		)

		logger.info(
			"Executing search query",
			extra = LoggingUtils.safe_query_dict(match_query)
		)

		# Execute 'find transients' query
		cursor = self._ampel_db \
			.get_collection('stock') \
			.find(
				match_query,
				{'_id': 1}, # indexed query
				no_cursor_timeout = True, # allow query to live for > 10 minutes
			) \
			.hint('_id_1_channels_1')
		
		# Count results
		if cursor.count() == 0:
			logger.info("No transient matches the given criteria")
			return None

		logger.info(
			f"{cursor.count()} transients match search criteria"
		)

		return cursor
