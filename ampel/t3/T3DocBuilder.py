#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Writer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 08.12.2021
# Last Modified Date: 09.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from datetime import datetime
from typing import Union, Optional, Iterable, Any, Sequence, Literal

from ampel.types import StockId, ChannelId, Tag, UBson, ubson
from ampel.abstract.AbsT3StageUnit import AbsT3StageUnit
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.utils import report_exception
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler
from ampel.content.T3Document import T3Document
from ampel.content.MetaRecord import MetaRecord
from ampel.struct.UnitResult import UnitResult
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.util.mappings import dictify
from ampel.util.tag import merge_tags
from ampel.util.hash import build_unsafe_dict_id

AbsT3s = Union[AbsT3ControlUnit, AbsT3StageUnit, AbsT3PlainUnit]


class T3Writer(ContextUnit):
	"""
	Base class for units whose output are saved into the t3 collection.
	Known subclass: stager classes, T3Executor
	"""

	channel: Optional[ChannelId] = None

	#: Whether t3 result should be added to t3 store once available
	propagate: bool = True

	#: name of the associated process
	process_name: str

	#: raise exceptions instead of catching and logging
	raise_exc: bool = True

	#: Note that if True, a T3 document will be created even if a t3 unit returns None
	save_stock_ids: bool = False

	#: If true, value of T3Document.config will be the config dict rather than its hash
	resolve_config: bool = False

	#: Tag(s) to be added to t3 documents if applicable (if t3 unit returns something)
	tag: Optional[Union[Tag, Sequence[Tag]]]

	#: If true, value of T3Document._id will be built using the 'elements' listed below.
	#: Note that 'tag' from unit results (UnitResult.tag) if defined, will be merged
	#: with potential stager tag(s). Note also that time is always appended.
	#: ex: {_id: [DipoleJob#Task#2] [T3CosmoDipole] [2021-10-20 10:38:48.889624]}
	#: ex: {_id: [T3CosmoDipole] [TAG_UNION2] [2021-10-20 10:42:41.123263]}
	human_id: Optional[list[Literal['process', 'taskindex', 'unit', 'tag', 'config', 'run']]]

	#: If true, a value will be set for T3Document.datetime
	human_timestamp: bool = False

	#: Used if human_timestamp is true
	human_timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"


	def __init__(self,
		logger: AmpelLogger,
		stock_updr: MongoStockUpdater, # potentially to be typed with an abstract class later
		event_hdlr: Optional[EventHandler] = None,
		**kwargs
	) -> None:

		super().__init__(**kwargs)

		# Non-serializable / not part of model / not validated; arguments
		self.logger = logger
		self.stock_updr = stock_updr
		self.event_hdlr = event_hdlr


	def handle_error(self, e: Exception) -> None:

		if self.event_hdlr:
			self.event_hdlr.add_extra(overwrite=True, success=False)
	
		if self.raise_exc:
			raise e

		# Try to insert doc into trouble collection (raises no exception)
		report_exception(
			self.context.db, self.logger, exc=e,
			process = self.stock_updr.process_name
		)


	def handle_t3_result(self,
		t3_unit: AbsT3s,
		res: Union[UBson, UnitResult],
		stocks: Optional[list[StockId]],
		ts: float
	) -> Optional[T3Document]:

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
				return self.craft_t3_doc(t3_unit, res, ts, stocks)
		elif res is not None or (res is None and self.save_stock_ids and stocks):
			return self.craft_t3_doc(t3_unit, res, ts, stocks)

		return None


	def flush(self, arg: Union[AbsT3s, Iterable[AbsT3s]], extra: Optional[dict[str, Any]] = None) -> None:

		for t3_unit in [arg] if isinstance(arg, (AbsT3ControlUnit, AbsT3StageUnit, AbsT3PlainUnit)) else arg:

			if (handlers := getattr(t3_unit.logger, 'handlers')) and handlers[0].buffer:
				handlers[0].forward(self.logger, extra=extra)
				self.logger.break_aggregation()

			if self.stock_updr.update_journal:
				self.stock_updr.flush()


	def craft_t3_doc(self,
		t3_unit: AbsT3s,
		res: Union[None, UBson, UnitResult],
		ts: float,
		stocks: Optional[list[StockId]] = None
	) -> T3Document:

		t3d: T3Document = {'process': self.process_name}
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
				ids.append("[%s]" % self.process_name)
			if 'taskindex' in self.human_id:
				ids.append("[#%s]" % self.process_name.split("#")[-1])
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
