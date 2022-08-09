#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/t3/T3DocBuilderModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.12.2021
# Last Modified Date:  20.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from ampel.types import Tag, ChannelId, OneOrMany
from ampel.base.AmpelBaseModel import AmpelBaseModel


class T3DocBuilderModel(AmpelBaseModel):
	"""
	Provides methods for handling UnitResult and generating a T3Document out of it
	"""

	channel: None | OneOrMany[ChannelId] = None

	#: tag: tag(s) to add to the :class:`~ampel.content.JournalRecord.JournalRecord` of each selected stock
	extra_journal_tag: None | OneOrMany[Tag] = None

	#: Record the invocation of this event in the stock journal
	update_journal: bool = True

	#: Whether t3 result should be added to t3 store once available
	propagate: bool = True

	#: Applies only for underlying T3ReviewUnits.
	#: Note that if True, a T3 document will be created even if a t3 unit returns None
	save_stock_ids: bool = False

	#: If true, value of T3Document.config will be the config dict rather than its hash
	resolve_config: bool = False

	#: Tag(s) to be added to t3 documents if applicable (if t3 unit returns something)
	tag: None | OneOrMany[Tag] = None

	#: If true, value of T3Document._id will be built using the 'elements' listed below.
	#: Note that 'tag' from unit results (UnitResult.tag) if defined, will be merged
	#: with potential stager tag(s). Note also that time is always appended.
	#: ex: {_id: [DipoleJob#Task#2] [T3CosmoDipole] [2021-10-20 10:38:48.889624]}
	#: ex: {_id: [T3CosmoDipole] [TAG_UNION2] [2021-10-20 10:42:41.123263]}
	human_id: None | list[Literal['process', 'taskindex', 'unit', 'tag', 'config', 'run']] = None

	#: If true, a value will be set for T3Document.datetime
	human_timestamp: bool = False

	#: Used if human_timestamp is true
	human_timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"
