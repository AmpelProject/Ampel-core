#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3AggregatingStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 08.12.2021
# Last Modified Date: 10.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pydantic import BaseModel
from typing import Union, Sequence, Generator, Optional, Any

from ampel.types import UBson
from ampel.t3.T3DocBuilder import T3DocBuilder
from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.content.MetaRecord import MetaRecord
from ampel.util.mappings import get_by_json_path


class TargetModel(BaseModel):
	unit: Optional[str]
	config: Union[None, int, str]
	code: Optional[int]
	field: Union[str, Sequence[str]]


class T3AggregatingStager(AbsT3Stager, T3DocBuilder):

	# Override
	paranoia: Optional[bool] = None # type: ignore
	save_stock_ids: bool = True

	split_tiers: bool = False
	t0: Union[None, TargetModel, Sequence[TargetModel]]
	t1: Union[None, TargetModel, Sequence[TargetModel]]
	t2: Union[None, TargetModel, Sequence[TargetModel]]


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> Optional[Generator[T3Document, None, None]]:

		t0 = [self.t0] if isinstance(self.t0, TargetModel) else self.t0
		t1 = [self.t1] if isinstance(self.t1, TargetModel) else self.t1
		t2 = [self.t2] if isinstance(self.t2, TargetModel) else self.t2

		if t0:
			t0d: dict[str, Any] = {}

		if t1:
			t1d: dict[str, Any] = {}

		if t2:
			t2d: dict[str, Any] = {}

		for ab in gen:

			sid = str(ab['id'])

			if t0:
				raise NotImplementedError("Please implement")

			if t1:
				raise NotImplementedError("Please implement")

			if t2:
				for model in t2:
					for t2doc in ab.get('t2') or []:
						if t2doc['unit'] != model.unit:
							continue
						if model.code is not None and t2doc['code'] != model.code:
							continue
						if body := self.get_t2_payload(t2doc['body'], t2doc['meta'], model.code):
							if model.field:
								for f in [model.field] if isinstance(model.field, str) else model.field:
									if (ret := get_by_json_path(body, f)):
										if sid not in t2d:
											t2d[sid] = {}
										if ret[0] == "*":
											t2d[sid].update(ret[1])
										else:
											t2d[sid][ret[0]] = ret[1]
							else:
								t2d[sid].update(ret)

		if t0 and not t1 and not t2:
			return self._craft(t0d, 't0', t3s)

		if t1 and not t0 and not t2:
			return self._craft(t1d, 't1', t3s)

		if t2 and not t1 and not t0:
			return self._craft(t2d, 't2', t3s)

		out: dict[str, Any] = {}

		if t0:
			self._upd(out, t0d, 't0')
		if t1:
			self._upd(out, t1d, 't1')
		if t2:
			self._upd(out, t2d, 't2')

		return self._craft(out, '', t3s)


	def _craft(self, d: dict[str, Any], s: str, t3s: T3Store) -> Generator[T3Document, None, None]:
		yield self.craft_t3_doc(
			self, # type: ignore
			{k: {s: v} for k, v in d.items()} if self.split_tiers else d,
			t3s,
			time(),
			[int(el) for el in d.keys()]
		)


	def _upd(self, out: dict[str, Any], d: dict[str, Any], s: str) -> None:

		if self.split_tiers:
			for k, v in d.items():
				out[k] = {s: v}
		else:
			for k, v in d.items():
				if k in out:
					out[k].update(v)
				else:
					out[k] = v


	def get_t2_payload(self,
		body: Optional[Sequence[UBson]],
		meta: Sequence[MetaRecord],
		code: Optional[int] = None
	) -> Optional[dict[str, Any]]:
		"""
		:returns: the content of the last array element of body associated with a meta code >= 0 or equals code arg.
		"""
		if not body:
			return None

		idx = len(
			[
				el for el in meta
				if el['tier'] == 2 and
				(el['code'] >= 0 if code is None else el['code'] == code)
			]
		) - 1

		if idx == -1:
			return None

		# A manual/admin $unset: {body: 1} was used to delete bad data
		if idx > len(body) - 1:
			idx = len(body) - 1

		if idx >= 0 and isinstance(body[idx], dict):
			return body[idx] # type: ignore[return-value] # remove when mypy gets smarter

		return None
