#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3BaseStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                08.12.2021
# Last Modified Date:  03.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator
from time import time
from typing import Any

from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsT3Unit import AbsT3Unit, T
from ampel.base.AmpelUnit import AmpelUnit
from ampel.content.T3Document import T3Document
from ampel.core.DocBuilder import DocBuilder
from ampel.core.EventHandler import EventHandler
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.struct.T3Store import T3Store
from ampel.struct.UnitResult import UnitResult
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator
from ampel.types import ChannelId, OneOrMany, StockId, Tag, Traceless, UBson
from ampel.util.mappings import dictify


class T3BaseStager(AbsT3Stager, DocBuilder, abstract=True):
	"""
	Base (abstract) class for several stagers provided by ampel.
	This class does not implement the method stage(...) required by AbsT3Stager,
	it is up to the subclass to do it according to requirements.
	"""


	logger: Traceless[AmpelLogger]
	event_hdlr: Traceless[EventHandler]

	#: tag: tag(s) to add to the :class:`~ampel.content.JournalRecord.JournalRecord` of each selected stock
	extra_journal_tag: None | OneOrMany[Tag] = None

	#: Record the invocation of this event in the stock journal
	update_journal: bool = True

	#: Whether t3 result should be added to t3 store once available
	propagate: bool = True

	#: Applies only for underlying T3ReviewUnits.
	#: Note that if True, a T3 document will be created even if a t3 unit returns None
	save_stock_ids: bool = False


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.stock_updr = MongoStockUpdater(
			ampel_db = self.context.db,
			tier = 3,
			run_id = self.event_hdlr.get_run_id(),
			process_name = self.event_hdlr.process_name,
			logger = self.logger,
			raise_exc = self.event_hdlr.raise_exc,
			extra_tag = self.extra_journal_tag,
			update_journal = self.update_journal,
			bump_updated = False
		)


	def get_unit(self, unit_model: UnitModel, chan: None | OneOrMany[ChannelId] = None) -> AbsT3Unit:

		# new_safe_logical_unit returns a T3 unit instantiated with
		# a logger based on DefaultRecordBufferingHandler
		return self.context.loader.new_safe_logical_unit(
			unit_model,
			unit_type = AbsT3Unit,
			logger = self.logger,
			_chan = self.channel or chan # type: ignore[arg-type] # to be improved when time allows
		)


	def handle_t3_result(self,
		t3_unit: AbsT3Unit,
		res: UBson | UnitResult,
		t3s: T3Store,
		stocks: None | list[StockId],
		ts: float,
		log_extra: None | dict[str, Any] = None
	) -> None | T3Document:

		# Let's consider logs as a result product
		if (buf_hdlr := getattr(t3_unit, '_buf_hdlr', None)) and buf_hdlr.buffer:
			buf_hdlr.forward(self.logger, extra=log_extra)

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
		t3_unit: AmpelUnit,
		res: None | UBson | UnitResult,
		t3s: T3Store,
		ts: float,
		stocks: None | list[StockId] = None
	) -> T3Document:

		t3d = super().craft_doc(self.event_hdlr, t3_unit, res, ts, doc_type=T3Document)
		if self.save_stock_ids and stocks:
			t3d['stock'] = stocks

		if t3s.session:
			t3d['session'] = dictify(t3s.session)

		return t3d


	def proceed(self,
		t3_unit: AbsT3Unit,
		view_generator: BaseViewGenerator[T],
		t3s: T3Store
	) -> Generator[T3Document, None, None]:
		"""
		Executes method 'process' of t3 unit with provided views generator and t3 store.
		Handles potential t3 unit result as well.
		"""

		try:
			self.logger.info("Running T3 unit", extra={'unit': t3_unit.__class__.__name__})
			ts = time()
			if (
				((ret := t3_unit.process(view_generator, t3s)) or self.save_stock_ids) and
				(x := self.handle_t3_result(t3_unit, ret, t3s, view_generator.get_stock_ids(), ts))
			):
				yield x
		except Exception as e:
			self.event_hdlr.handle_error(e, self.logger)
		finally:
			if self.stock_updr.update_journal:
				self.stock_updr.flush()
