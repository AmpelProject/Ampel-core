#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/DocBuilder.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                04.04.2023
# Last Modified Date:  04.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from datetime import datetime, timezone
from typing import Literal, TypeVar

from ampel.abstract.AbsUnitResultAdapter import AbsUnitResultAdapter
from ampel.base.AmpelUnit import AmpelUnit
from ampel.content.MetaRecord import MetaRecord
from ampel.content.T3Document import T3Document
from ampel.content.T4Document import T4Document
from ampel.core.ContextUnit import ContextUnit
from ampel.core.EventHandler import EventHandler
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.model.UnitModel import UnitModel
from ampel.struct.UnitResult import UnitResult
from ampel.types import ChannelId, OneOrMany, Tag, UBson, ubson
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.tag import merge_tags

T = TypeVar("T", T3Document, T4Document)


class DocBuilder(ContextUnit):

	channel: None | OneOrMany[ChannelId] = None

	#: If true, value of T3/T4Document.config will be the config dict rather than its hash
	resolve_config: bool = False

	#: Tag(s) to be added to t3/t4 documents if applicable (if unit returns something)
	tag: None | OneOrMany[Tag] = None

	#: If set, value of T3/T4Document._id will be built using the 'elements' listed below.
	human_id: None | list[Literal['process', 'taskindex', 'unit', 'tag', 'config', 'run']] = None

	#: If true, a value will be set for T3/T4Document.datetime
	human_timestamp: bool = False

	#: Used if human_timestamp is true
	human_timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f"


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.adapters: dict[int, AbsUnitResultAdapter] = {}


	def get_adapter_instance(self, model: UnitModel, event_hdlr: EventHandler) -> AbsUnitResultAdapter:
		config_id = build_unsafe_dict_id(model.dict())

		if (
			config_id not in self.adapters or
			self.adapters[config_id].run_id != event_hdlr.get_run_id()
		):
			unit_instance = self.context.loader.new_context_unit(
				model = model,
				context = self.context,
				run_id = event_hdlr.get_run_id(),
				sub_type = AbsUnitResultAdapter,
			)
			self.adapters[config_id] = unit_instance

		return self.adapters[config_id]

	
	def craft_doc(self,
		event_hdlr: EventHandler,
		unit: AmpelUnit,
		res: None | UBson | UnitResult,
		ts: float,
		doc_type: type[T]
	) -> T:

		d: T = {'process': event_hdlr.process_name} # type: ignore[typeddict-item]
		actact = MetaActionCode(0)
		now = datetime.now(tz=timezone.utc)

		if self.human_timestamp:
			d['datetime'] = now.strftime(self.human_timestamp_format)

		d['unit'] = unit.__class__.__name__
		d['code'] = actact

		conf = unit._get_trace_content()  # noqa: SLF001
		meta: MetaRecord = {
			'run': event_hdlr.get_run_id(),
			'ts': int(now.timestamp()),
			'duration': now.timestamp() - ts
		}

		confid = build_unsafe_dict_id(conf)
		self.context.db.add_conf_id(confid, conf)

		# Live dangerously
		if confid not in self.context.config._config['confid']:  # noqa: SLF001
			dict.__setitem__(self.context.config._config['confid'], confid, conf)  # noqa: SLF001

		d['confid'] = confid

		if self.resolve_config:
			d['config'] = conf

		if self.channel:
			d['channel'] = self.channel
			actact |= MetaActionCode.ADD_CHANNEL

		d['code'] = DocumentCode.NOT_SET
		d['meta'] = meta # note: mongodb maintains key order

		if isinstance(res, UnitResult):

			if res.code:
				d['code'] = res.code
				actact |= MetaActionCode.SET_UNIT_CODE
			else:
				actact |= MetaActionCode.SET_CODE

			if res.tag:
				if self.tag:
					d['tag'] = merge_tags(self.tag, res.tag)
				else:
					d['tag'] = res.tag
			elif self.tag:
				d['tag'] = self.tag

			if res.body:

				if res.adapter_model:
					res = self.get_adapter_instance(res.adapter_model, event_hdlr).handle(res)

				d['body'] = res.body
				actact |= MetaActionCode.ADD_BODY

		else:

			if self.tag:
				d['tag'] = self.tag

			# bson
			if isinstance(res, ubson):
				d['body'] = res
				actact |= (MetaActionCode.ADD_BODY | MetaActionCode.SET_CODE)

			else:
				actact |= MetaActionCode.SET_CODE

		meta['activity'] = [{'action': actact}]

		if self.human_id:
			ids = []
			if 'process' in self.human_id:
				ids.append(f"[{event_hdlr.process_name}]")
			if 'taskindex' in self.human_id:
				ids.append("[#{}]".format(event_hdlr.process_name.split("#")[-1]))
			if 'unit' in self.human_id:
				ids.append(f"[{unit.__class__.__name__}]")
			if 'tag' in self.human_id and d.get('tag'):
				ids.append("[%s]" % (d['tag'] if isinstance(d['tag'], int | str) \
					else " ".join(d['tag']))) # type: ignore[arg-type]
			if 'config' in self.human_id:
				ids.append(f"[{confid}]")
			if 'run' in self.human_id:
				ids.append(f"[{event_hdlr.get_run_id()}]")
			ids.append(datetime.now(tz=timezone.utc).strftime(self.human_timestamp_format))
			d['_id'] = " ".join(ids)

		return d
