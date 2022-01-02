#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/T3DocBuilder.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                08.12.2021
# Last Modified Date:  18.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from datetime import datetime
from typing import Any
from collections.abc import Iterable

from ampel.types import Traceless, StockId, UBson, ubson
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.model.t3.T3DocBuilderModel import T3DocBuilderModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler
from ampel.content.T3Document import T3Document
from ampel.content.MetaRecord import MetaRecord
from ampel.struct.UnitResult import UnitResult
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.view.T3Store import T3Store
from ampel.util.mappings import dictify
from ampel.util.tag import merge_tags
from ampel.util.hash import build_unsafe_dict_id

AbsT3s = AbsT3ControlUnit | AbsT3ReviewUnit | AbsT3PlainUnit


class T3DocBuilder(T3DocBuilderModel, ContextUnit):
	"""
	Provides methods for handling UnitResult and generating a T3Document out of it
	"""

	logger: Traceless[AmpelLogger]
	event_hdlr: Traceless[EventHandler]


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.stock_updr = MongoStockUpdater(
			ampel_db = self.context.db,
			tier = 3,
			run_id = self.event_hdlr.run_id,
			process_name = self.event_hdlr.process_name,
			logger = self.logger,
			raise_exc = self.event_hdlr.raise_exc,
			extra_tag = self.extra_journal_tag,
			update_journal = self.update_journal,
			bump_updated = False
		)


	def handle_t3_result(self,
		t3_unit: AbsT3s,
		res: UBson | UnitResult,
		t3s: T3Store,
		stocks: None | list[StockId],
		ts: float
	) -> None | T3Document:

		# Let's consider logs as a result product
		if (buf_hdlr := getattr(t3_unit, '_buf_hdlr')) and buf_hdlr.buffer:
			buf_hdlr.forward(self.logger)

		if isinstance(res, UnitResult):
			if stocks and res.journal:
				self.stock_updr.add_journal_record(
					stock = stocks, # used to match stock docs
					jattrs = res.journal,
					unit = t3_unit.__class__.__name__,
					action_code = JournalActionCode.T3_ADD_DOC
				)
			if res.body is not None or res.code is not None:
				return self.craft_t3_doc(t3_unit, res, t3s, ts, stocks)
		elif res is not None or (res is None and self.save_stock_ids and stocks):
			return self.craft_t3_doc(t3_unit, res, t3s, ts, stocks)

		return None


	def craft_t3_doc(self,
		t3_unit: AbsT3s,
		res: None | UBson | UnitResult,
		t3s: T3Store,
		ts: float,
		stocks: None | list[StockId] = None
	) -> T3Document:

		t3d: T3Document = {'process': self.event_hdlr.process_name}
		actact = MetaActionCode(0)
		now = datetime.now()

		if self.human_timestamp:
			t3d['datetime'] = now.strftime(self.human_timestamp_format)

		t3d['unit'] = t3_unit.__class__.__name__
		t3d['code'] = actact

		conf = dictify(t3_unit._trace_content)
		meta: MetaRecord = {'ts': int(now.timestamp()), 'duration': time() - ts}

		confid = build_unsafe_dict_id(conf)
		self.context.db.add_conf_id(confid, conf)

		# Live dangerously
		if confid not in self.context.config._config['confid']:
			dict.__setitem__(self.context.config._config['confid'], confid, conf)

		t3d['confid'] = confid

		if self.resolve_config:
			t3d['config'] = conf

		if self.channel:
			t3d['channel'] = self.channel
			actact |= MetaActionCode.ADD_CHANNEL

		if self.save_stock_ids and stocks:
			t3d['stock'] = stocks

		t3d['code'] = DocumentCode.OK
		t3d['meta'] = meta # note: mongodb maintains key order

		if t3s.session:
			t3d['session'] = dictify(t3s.session)

		if isinstance(res, UnitResult):

			if res.code:
				t3d['code'] = res.code
				actact |= MetaActionCode.SET_UNIT_CODE
			else:
				actact |= MetaActionCode.SET_CODE

			if res.tag:
				if self.tag:
					t3d['tag'] = merge_tags(self.tag, res.tag) # type: ignore
				else:
					t3d['tag'] = res.tag
			elif self.tag:
				t3d['tag'] = self.tag

			if res.body:
				t3d['body'] = res.body
				actact |= MetaActionCode.ADD_BODY

		else:

			if self.tag:
				t3d['tag'] = self.tag

			# bson
			if isinstance(res, ubson):
				t3d['body'] = res
				actact |= (MetaActionCode.ADD_BODY | MetaActionCode.SET_CODE)

			else:
				actact |= MetaActionCode.SET_CODE

		meta['activity'] = [{'action': actact}]

		if self.human_id:
			ids = []
			if 'process' in self.human_id:
				ids.append("[%s]" % self.event_hdlr.process_name)
			if 'taskindex' in self.human_id:
				ids.append("[#%s]" % self.event_hdlr.process_name.split("#")[-1])
			if 'unit' in self.human_id:
				ids.append("[%s]" % t3_unit.__class__.__name__)
			if 'tag' in self.human_id and 'tag' in t3d:
				ids.append("[%s]" % (t3d['tag'] if isinstance(t3d['tag'], (int, str)) else " ".join(t3d['tag']))) # type: ignore[arg-type]
			if 'config' in self.human_id:
				ids.append("[%s]" % build_unsafe_dict_id(conf))
			if 'run' in self.human_id:
				ids.append("[%s]" % self.stock_updr.run_id) # not great
			ids.append(now.strftime(self.human_timestamp_format))
			t3d['_id'] = " ".join(ids)

		return t3d


	def flush(self, arg: AbsT3s | Iterable[AbsT3s], extra: None | dict[str, Any] = None) -> None:

		for t3_unit in [arg] if isinstance(arg, (AbsT3ControlUnit, AbsT3ReviewUnit, AbsT3PlainUnit)) else arg:

			if (handlers := getattr(t3_unit.logger, 'handlers')) and handlers[0].buffer:
				handlers[0].forward(self.logger, extra=extra)
				self.logger.break_aggregation()

			if self.stock_updr.update_journal:
				self.stock_updr.flush()
