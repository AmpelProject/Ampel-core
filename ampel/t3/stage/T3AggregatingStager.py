#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3AggregatingStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                08.12.2021
# Last Modified Date:  16.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from ampel.base.AmpelBaseModel import AmpelBaseModel
from typing import Any
from collections.abc import Generator, Sequence

from ampel.types import UBson, OneOrMany
from ampel.t3.T3DocBuilder import T3DocBuilder
from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.content.MetaRecord import MetaRecord
from ampel.util.mappings import get_by_json_path


class TargetModel(AmpelBaseModel):
	unit: None | str
	config: None | int | str
	code: None | int
	field: OneOrMany[str]


class T3AggregatingStager(AbsT3Stager, T3DocBuilder):
	"""
	Example:

	unit: T3AggregatingStager
	config:
	  t2:
	  - unit: T2NedTap
	    field: "data[0].*"
	  - unit: T2NedSNCosmo
	  field:
	  - "data[0].fit_results"
	  - "data[0].covariance"

	will create a new t3 doc in the DB, whose body will contain the aggregated results.
	The doc could look like below (note that ampel ids were stringified to comply with BSON requirements):

	{
	  ...
	  'body': {
	    "33876" : {
	       "prefname" : "WISEA J235434.02+154441.8",
	       "pretype" : "G",
	       "ra" : 358.6417460104,
	       "dec" : 15.7449859573,
	       ...
	       "fit_results" : {
	         "z" : 0.07458627,
	         "t0" : 2459226.42052562,
	         "x0" : 0.000359877303537212,
	         ...
	       },
	       "covariance": [
	         [0.525561894902874, 5.08414469979e-06, 0.103650196867142, -0.0141356930137254],
	         ...
	       ],
	       ...
	   },
	   "496964" : {
	     "prefname" : "WISEA J114639.05+421201.3",
	     "pretype" : "G",
	     "ra" : 176.6627824999,
	     "dec" : 42.2004549186,
	     ...
	     "fit_results" : {
	       "z" : 0.0512447,
	       "t0" : 2459227.16179618,
	       "x0" : 0.000925982418211906,
	       ...
	     },
	     "covariance" : [
	       [0.0427179910190306, 7.18629916088547e-07, 0.000100038844775205, -0.000266005925059596],
	       ...
	     ],
	     ...
	  }
	...
	"""

	# Override
	save_stock_ids: bool = True

	#: Only applies to doc output
	split_tiers: bool = False

	t0: None | OneOrMany[TargetModel]
	t1: None | OneOrMany[TargetModel]
	t2: None | OneOrMany[TargetModel]


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:

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
		body: None | Sequence[UBson],
		meta: Sequence[MetaRecord],
		code: None | int = None
	) -> None | dict[str, Any]:
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
