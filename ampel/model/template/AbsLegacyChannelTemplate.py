#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/AbsLegacyChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 24.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from pydantic import validator
from typing import List, Dict, Any, Union, Tuple, Optional, Sequence
from ampel.log.AmpelLogger import AmpelLogger
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate


class T2UnitModel(UnitModel):
	"""
	Precursor that ProcessMorpher can turn into a T2IngestModel
	"""
	#: Ingester options
	ingest: Optional[Dict[str, Any]]
	#: Filter result codes that should trigger this T2. If not specified, T2
	#: documents will be created in response to *any* passing alert.
	group: Union[int, List[int]] = []


class AbsLegacyChannelTemplate(AbsChannelTemplate, abstract=True):
	"""
	Abstract class whose purpose is to maintain compatibility with channel
	definitions created for ampel versions < 0.7.
	This class must be subclassed.
	
	Known subclass: :class:`~ampel.model.ZTFLegacyChannelTemplate.ZTFLegacyChannelTemplate`
	"""
	#: How to treat photopoints be treated once a transient has been accepted.
	#
	#: - false: apply filter to all photopoints
	#: - true or "live": bypass filter once a transient has been accepted once
	auto_complete: Union[bool, str]
	#: Filter to apply to incoming datapoints
	t0_filter: UnitModel
	#: T2 units to trigger when transient is updated. Dependencies of tied
	#: units will be added automatically.
	t2_compute: List[T2UnitModel] = []
	#: T3 processes bound to this channel. These may be use templates, such as
	#: :class:`~ampel.model.template.PeriodicSummaryT3.PeriodicSummaryT3`.
	t3_supervise: List[Dict[str, Any]] = []


	@validator('t3_supervise', 't2_compute', pre=True, each_item=False)
	def cast_to_list_if_required(cls, v):
		if isinstance(v, dict):
			return [v]
		return v


	@validator('auto_complete')
	def make_auto_stock_match(cls, v):
		if v is True or v == 'live':
			return {
				'filter': 'bypass',
				'update_rej': True,
				'retro_complete': True
			}
		else:
			return {
				'filter': 'overrule',
				'update_rej': False,
				'retro_complete': False
			}


	# Mandatory implementation
	def get_channel(self, logger: AmpelLogger) -> Dict[str, Any]:
		d = self.dict(by_alias=True)
		for k in ("auto_complete", "t0_filter", "t2_compute", "t3_supervise"):
			if k in d:
				del d[k]
		return d


	def craft_t0_process(self,
		first_pass_config: FirstPassConfig,
		controller: Dict[str, Any],
		stock_ingester: Union[str, Tuple[str, Dict[str, Any]]],
		t0_ingester: Union[str, Tuple[str, Dict[str, Any]]],
		t1_ingester: Optional[Union[str, Tuple[str, Dict[str, Any]]]] = None,
		t1_standalone_ingester: Optional[Union[str, Tuple[str, Dict[str, Any]]]] = None,
		t2_state_ingester: Optional[Union[str, Tuple[str, Dict[str, Any]]]] = None,
		t2_point_ingester: Optional[Union[str, Tuple[str, Dict[str, Any]]]] = None,
		t2_stock_ingester: Optional[Union[str, Tuple[str, Dict[str, Any]]]] = None,
		t2_compute_from_t0: List[T2UnitModel] = [],
		t2_compute_from_t1: List[T2UnitModel] = [],
	) -> Dict[str, Any]:
		"""
		This method needs a reference to a FirstPassConfig dict because
		config information might be needed during the template transforming process.
		For example, legacy channel templates (such as ZTFLegacyChannelTemplate)
		allow users to reference any kind of t2 units under the root config section 't2_compute'.
		The AlertProcessor however, requires different configuration paths for "state T2s" and "point T2s".
		The underlying templates will thus have to sort T2s based on their respective abstract classes,
		and for this, the ampel configuration is required.

		:param stock_ingester: unit_class or (unit_class, config dict)
		:param t0_ingester: unit_class or (unit_class, config dict)
		:param t1_ingester: unit_class or (unit_class, config dict)
		:param t2_state_ingester: unit_class or (unit_class, config dict)
		:param t2_point_ingester: unit_class or (unit_class, config dict)
		:param t2_stock_ingester: unit_class or (unit_class, config dict)
		:param t2_compute_from_t0: units to schedule on t0_add
		:param t2_compute_from_t1: units to schedule on t1_combine
		"""

		ret: Dict[str, Any] = {
			"tier": 0,
			"schedule": ["super"],
			"active": self.active,
			"distrib": self.distrib,
			"source": self.source,
			"channel": self.channel,
			"name": f"{self.channel}|T0|{self.template}",
			"controller": controller,
			"processor": {
				"unit": "AlertProcessor",
				"config": {
					"directives": [{
						"channel": self.channel,
						"stock_match": self.auto_complete,
						"filter": self.t0_filter.dict(exclude_unset=True, by_alias=True),
						"t0_add": self._get_dict(t0_ingester),
						"stock_update": self._get_dict(stock_ingester)
					}]
				}
			}
		}

		directives = ret['processor']['config']['directives'][0]

		self.check_tied_units(
			t2_compute_from_t0 + t2_compute_from_t1,
			first_pass_config
		)

		if t2_state_units := self.get_units(
			t2_compute_from_t0,
			[
				"AbsStateT2Unit",
				"AbsCustomStateT2Unit",
				"AbsTiedStateT2Unit",
				"AbsTiedCustomStateT2Unit",
			],
			first_pass_config
		):

			if t1_ingester is None:
				raise ValueError("Template processing requires parameter 't1_ingester'")

			if t2_state_ingester is None:
				raise ValueError("Template processing requires parameter 't2_state_ingester'")

			directives['t0_add']['t1_combine'] = [
				self._get_dict(
					t1_ingester,
					t2_compute = self._get_dict(
						t2_state_ingester,
						units = t2_state_units
					)
				)
			]

		if t2_state_units := self.get_units(
			t2_compute_from_t1,
			[
				"AbsStateT2Unit",
				"AbsCustomStateT2Unit",
				"AbsTiedStateT2Unit",
				"AbsTiedCustomStateT2Unit",
			],
			first_pass_config
		):

			if t1_standalone_ingester is None:
				raise ValueError("Template processing requires parameter 't1_standalone_ingester'")

			if t2_state_ingester is None:
				raise ValueError("Template processing requires parameter 't2_state_ingester'")

			directives['t1_combine'] = [
				self._get_dict(
					t1_standalone_ingester,
					t2_compute = self._get_dict(
						t2_state_ingester,
						units = t2_state_units
					)
				)
			]

		if self.get_units(t2_compute_from_t1, ["AbsStockT2Unit", "AbsPointT2Unit"], first_pass_config):
			raise NotImplementedError("Stock and Point T2 units can't be scheduled from standalone T1 processes")

		if t2_stock_units := self.get_units(t2_compute_from_t0, "AbsStockT2Unit", first_pass_config):

			if t2_stock_ingester is None:
				raise ValueError("Template processing requires parameter 't2_stock_ingester'")

			directives['t2_compute'] = self._get_dict(
				t2_stock_ingester, units = t2_stock_units
			)

		if t2_point_units := self.get_units(t2_compute_from_t0, "AbsPointT2Unit", first_pass_config):

			if t2_point_ingester is None:
				raise ValueError("Template processing requires parameter 't2_point_ingester'")

			directives['t0_add']['t2_compute'] = self._get_dict(
				t2_point_ingester, units = t2_point_units
			)

		return ret


	def _get_dict(self, arg: Union[str, Tuple[str, Dict[str, Any]]], **kwargs) -> Dict[str, Any]:
		""" internal use """
		if isinstance(arg, tuple):
			if arg[1]:
				return {"unit": arg[0], "config": arg[1], **kwargs}
			return {"unit": arg[0], **kwargs}
		return {"unit": arg, **kwargs}


	def check_tied_units(self,
		all_t2_units: List[T2UnitModel],
		first_pass_config: FirstPassConfig
	) -> None:
		"""
		:raises: ValueError if tied t2 units are present in t2_units but the requred t2 units are not present in t2_compute
		"""
		tied_units: List[T2UnitModel] = []
		for el in all_t2_units:
			if "AbsTiedT2Unit" in first_pass_config['unit']['base'][el.unit_name]['base']:
				tied_units.append(el) # type: ignore
			
		s = set()
		for tied_unit in tied_units:
			klass = getattr(
				import_module(
					first_pass_config['unit']['base'][tied_unit.unit_name]['fqn']
				),
				tied_unit.unit_name
			)
			s.update(klass.get_tied_unit_names())

		if missing_deps := s.difference({u.unit for u in all_t2_units}):
			raise ValueError(
				f"Following t2 unit(s) must be defined (required by "
				f"tied units): {list(missing_deps)}"
			)
		
		def as_unitmodel(t2_unit_model: T2UnitModel) -> UnitModel:
			return UnitModel(**{k: v for k,v in t2_unit_model.dict().items() if k in UnitModel.__fields__})

		for tied_unit in tied_units:
			if (
				isinstance(tied_unit.config, dict)
				and (t2_deps := (tied_unit.config.get("t2_dependency") or []))
			):
				for t2_dep in t2_deps:
					dependency_config = UnitModel(unit=t2_dep["unit"], config=t2_dep.get("config"))
					candidates = [as_unitmodel(unit) for unit in all_t2_units if unit.unit_name == dependency_config.unit_name]
					if not any((c == dependency_config for c in candidates)):
						raise ValueError(
							f"Unit {tied_unit.unit_name} depends on unit {dependency_config.dict()}, "
							f"which was not configured. Possible matches are: "
							f"{[c.dict() for c in candidates]}"
						)


	def get_units(self,
		units: Sequence[UnitModel], abs_unit: Union[str, List[str]],
		first_pass_config: FirstPassConfig
	) -> List[Dict]:
		"""
		:returns: unit defintions (dict) that are subclass of the provided abstract class name.
		"""

		if isinstance(abs_unit, str):
			abs_unit = [abs_unit]

		return [
			el.dict(exclude_unset=True, by_alias=True)
			for el in units
			if any(
				unit in first_pass_config['unit']['base'][el.unit]['base']
				for unit in abs_unit
			)
		]
