#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/aux/ComboDictModifier.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                21.02.2020
# Last Modified Date:  21.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal, Any
from collections.abc import Container, Callable, Sequence
from ampel.abstract.AbsApplicable import AbsApplicable
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.log import AmpelLogger, VERBOSE
from ampel.util.collections import to_set
from ampel.view.ReadOnlyDict import ReadOnlyDict


class ComboDictModifier(AbsApplicable):
	"""
	Note: cannot make different modifications of the same ast at different depth
	"""

	class DeleteModel(AmpelBaseModel):
		"""
		ex: for a given dict instance *d*, the model:
			{'op': 'delete', 'key': ['a', 'b.c']}
		will be equivalent to:
			del d['a']
			del d['b']['c']
		"""
		op: Literal['delete']
		key: Sequence[str]

		def __init__(self, **kwargs):
			if 'key' in kwargs and isinstance(kwargs['key'], str):
				kwargs['key'] = [kwargs['key']]
			AmpelBaseModel.__init__(self, **kwargs)


	class KeepOnlyModel(AmpelBaseModel):
		"""
		ex: for a given dict instance *d*, the model:
		{'op': 'keep_only', 'key': 'a.b', 'keep': ['y', 'z']}
		will keep only the keys 'y' and 'z' of d['a']['b']
		"""
		op: Literal['keep_only']
		key: None | str
		keep: int |  str | Sequence[int | str]


	class ClassModifyModel(AmpelBaseModel):
		"""
		ex: for a given dict instance *d*, the model:
			{'op': 'modify', 'key': 'a.b', 'unit': 'SetIntersector', config: {'value': [1, 2]}}
		will replace the value of d['a']['b'] with the intersection of the value of d['a']['b']
		and the value provided by config.value
		"""
		op: Literal['modify']
		key: str
		unit: str
		config: dict[str, Any]


	class FuncModifyModel(AmpelBaseModel):
		op: Literal['modify']
		key: str
		func: Callable[[Any], Any]


	logger: AmpelLogger
	modifications: Sequence[DeleteModel | KeepOnlyModel | ClassModifyModel | FuncModifyModel]

	# Whether fields can be directly altered/modified or not
	# If not, new dict instances must be created to modify existing dicts
	unalterable: bool = False

	# Whether to cast structures into immutables objects after modification
	freeze: bool = True


	def __init__(self, **kwargs) -> None:
		"""
		:param unalterable: whether fields can be directly altered/modified or not
		If not, new dict instances must be created to modify existing dicts

		:param freeze: whether to cast structures into immutables objects after modification

		:param modifications: sequence of modifications to be performed on the dict.
		Note that different modifications of the same ast at different depth is not possible
		For more information, see docstrings of internal static models:
		DeleteModel, KeepOnlyModel, ClassModifyModel (inherits AbsApplicable), FuncModifyModel
		"""

		super().__init__(**kwargs)

		# Dict ops on root dict keys
		self._mods: dict[str, list[Callable[[Any], Any]]] = {}
		self._dels: set[str] = set()
		self._root_ko: Container[str] = set()
		self._depth1_ko: dict[str, Container[str]] = {}

		# Mypy does not yet support recursive types

		self._nested_dels: dict[str, Any] = {}
		self._nested_ko: dict[str, Any] = {}
		self._nested_mods: dict[str, Any] = {}

		for f in self.modifications:

			if isinstance(f, self.DeleteModel):

				for k in f.key:
					if "." not in k: # no nesting
						self._dels.add(k)
					else:
						# creates: {'a':{}, 'b': {'c': {}, 'd': {'e': {}}}}
						d = self._nested_dels
						for kk in k.split("."):
							if kk not in d:
								d[kk] = {}
							d = d[kk]

			elif isinstance(f, self.KeepOnlyModel):

				# "Root" keep only
				if not f.key:
					self._root_ko.update(
						to_set(f.keep)
					)
					continue

				# Non-nested key
				if "." not in f.key:
					if f.key in self._depth1_ko:
						raise ValueError("'keep only' dict operations must be atomar")
					self._depth1_ko[f.key] = to_set(f.keep)
					continue

				# creates: {'a': set(['foo', 'bar', ...]), 'b': {'c': set(['foo', 'bar', ...]), ...}}
				keys = f.key.split(".")
				d = self._nested_ko
				for k in keys[:-1]:
					if kk not in d:
						d[kk] = {}
					d = d[kk]
				d[keys[-1]] = to_set(f.keep)

			elif isinstance(f, (self.ClassModifyModel, self.FuncModifyModel)):

				if isinstance(f, self.ClassModifyModel):
					unit = AuxUnitRegister.new_unit(
						class_name = f.unit,
						sub_type = AbsApplicable,
						logger = self.logger,
						**f.config
					)
					func = unit.apply
				else:
					func = f.func # type: ignore

				# Non-nested key
				if "." not in f.key:
					if f.key not in self._mods:
						self._mods[f.key] = []
					self._mods[f.key].append(func)
					continue

				# creates: {'a': [<AbsApplicable>, ..], 'b': {'c': [<AbsApplicable>], ...}}
				keys = f.key.split(".")
				d = self._nested_mods
				for k in keys[:-1]:
					if kk not in d:
						d[kk] = {}
					d = d[kk]
				if keys[-1] not in d:
					d[keys[-1]] = []
				d[keys[-1]].append(func)

			else:
				raise ValueError("Unrecognized config model")

		# Optimization
		if self._nested_mods and self._mods:

			for k in self._mods:
				if k in self._nested_mods:
					if isinstance(self._nested_mods[k], list):
						self._nested_mods[k].append(self._mods[k])
					else:
						raise ValueError("Cannot perform multiple modifications on the same ast")
				else:
					self._nested_mods[k] = [self._mods[k]]
			self._mods = {}


		ops: dict[str, Any] = {
			# Ops handling non-nested keys
			"apply_root_delete": self._dels,
			"apply_root_modify": self._mods,
			"apply_root_keep_only": self._root_ko,
			"apply_depth1_keep_only": self._depth1_ko,

			# Ops handling nested keys
			"apply_delete": self._nested_dels,
			"apply_modify": self._nested_mods,
			"apply_keep_only": self._nested_ko,
		}

		if self.logger.verbose:
			self.logger.log(VERBOSE, "Following dict operations will be applied")
			for k, v in ops.items():
				if v:
					self.logger.log(VERBOSE, f" -> {k} ({len(v)})")

		self.ops = [getattr(self, k) for k, v in ops.items() if v]

		# Optimization
		different_ops = sum([1 for instructions in ops.values() if instructions])
		if different_ops == 1:
			for k, v in ops.items():
				if v:
					self.apply = getattr(self, k) # type: ignore
					break


	# =========================================
	# Modifications of non-nested dict elements
	# =========================================

	# Delete dict elements at root level
	def apply_root_delete(self, d: dict) -> dict:
		""" deletes given keys of dict instance """

		if self.unalterable:
			d = {k: v for k, v in d.items() if k not in self._dels}
		else:
			di = dict.__delitem__
			for k in self._dels:
				if k in d:
					di(d, k)

		return ReadOnlyDict(d) if self.freeze else d


	# "Keep only" specified dict keys at root level
	def apply_root_keep_only(self, d: dict) -> dict:
		"""
		Deletes all but certain dict keys of a dict (at root level).
		For example, applying model: {'op': 'delete', 'keep': ['a']}
		to dict: {'a': {'b', 'c'}, 'd': {'e': {'f'}}}
		will result in: {'a': {'b', 'c'}}
		"""
		dd = {k: v for k, v in d.items() if k in self._root_ko}
		return ReadOnlyDict(dd) if self.freeze else dd


	# "Keep only" for keys with depth equals 1
	def apply_depth1_keep_only(self, d: dict) -> dict:
		"""
		Deletes all but certain dict keys for a given given root keys.
		For example, applying model:
			{'op': 'delete', 'key': 'a', 'keep': ['b']}
		to dict:
			{'a': {'b', 'c'}, 'd': {'e': {'f'}}}
		will result in:
			{'a': {'b'}, 'd': {'e': {'f'}}}
		Note 1: this method does not support nesting, meaning that the
		value associated with 'key' in the model above cannot contain dots
		Note 2: Model field 'key' must target a dict instance (not a list or set)
		"""
		if self.unalterable:
			d = d.copy()

		dsi = dict.__setitem__
		for k in self._depth1_ko:
			dsi(d, k, {kk: d[k][kk] for kk in d[k] if kk in self._depth1_ko[k]})

		return ReadOnlyDict(d) if self.freeze else d


	# Modifications of dict elements at root level
	def apply_root_modify(self, d: dict) -> dict:
		""" Modifies root elements of dict instances """

		if self.unalterable:
			d = d.copy()

		dsi = dict.__setitem__
		for k, mods in self._mods.items():
			if k in d:
				for mod in mods:
					dsi(d, k, mod(d[k]))

		return ReadOnlyDict(d) if self.freeze else d


	# =====================================
	# Modifications of nested dict elements
	# =====================================

	# Delete nested dict elements of any depth
	def apply_delete(self, d: dict, deletions: None | dict = None) -> dict:
		"""
		Method apply_delete:
		:param deletions: if None, self._nested_dels is used (set by constructor)
		deletions example: {'a':{}, 'b': {'c': {}, 'd': {'e': {}}}}
		-> empty dicts mark entries to be deleted
		"""
		if not deletions:
			deletions = self._nested_dels

		if self.unalterable:
			# suppreses a
			d = {k: d[k] for k in d if k not in deletions or deletions.get(k)}

		ddi = dict.__delitem__
		for k in deletions:
			if k in d:
				if not deletions[k]: # suppresses a
					ddi(d, k)
				else:
					# suppresses b.c and b.d.e
					self.apply_delete(d[k], deletions[k])
					if not d[k]: # Suppress empty dicts
						ddi(d, k)

		return ReadOnlyDict(d) if self.freeze else d


	# Delete nested dict elements of any depth
	def apply_modify(self, d: dict, modifications: None | dict = None) -> dict:
		"""
		Method apply_modify:
		:param modifications: if None, self._nested_mods is used (set by constructor)
		modifications example: {'a':[<method>], 'b': {'c': [<method>], 'd': {'e': [<method>]}}}
		"""
		if self.unalterable:
			d = d.copy()

		dsi = dict.__setitem__

		for k, mod in (modifications or self._nested_mods).items():
			if k in d:
				if isinstance(mod, dict):
					dsi(d, k, self.apply_modify(d[k], mod))
				else:
					for mod in mod:
						dsi(d, k, mod(d[k]))

		return ReadOnlyDict(d) if self.freeze else d


	# Keep only dict elements of any depth
	######################################

	def apply_keep_only(self, d: dict, nested_map: None | dict = None) -> dict:
		"""
		Method apply_keep_only:
		:param nested_map: if None, self._nested_ko is used (set by constructor)
		modifications example: {'a':<set>, 'b': {'c': <set>, 'd': {'e': <set>}}}
		"""

		if self.unalterable:
			d = d.copy()

		dsi = dict.__setitem__
		for k, v in (nested_map or self._nested_ko).items():
			if k in d:
				if isinstance(v, dict):
					dsi(d, k, self.apply_keep_only(d[k], v))
				else: # should be a set
					dsi(d, k, {kk: vv for kk, vv in d[k].items() if kk in v})

		return ReadOnlyDict(d) if self.freeze else d


	def apply(self, d: dict) -> None | dict:
		""" Modifies provided dict according to configured operations """
		try:
			for op in self.ops:
				d = op(d)
			return d
		except Exception as e:
			self.logger.error(
				f"Exception occured in {self.__class__.__name__}\n",
				exc_info=e
			)

		return None
