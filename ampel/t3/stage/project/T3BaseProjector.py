#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/project/T3BaseProjector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.01.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable, Union, Optional, List, Dict, Sequence, Callable, Any, Set
from ampel.log import AmpelLogger, VERBOSE
from ampel.struct.AmpelBuffer import AmpelBuffer, BufferKey
from ampel.abstract.AbsApplicable import AbsApplicable
from ampel.model.UnitModel import UnitModel
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.model.StrictModel import StrictModel
from ampel.abstract.AbsT3Projector import AbsT3Projector


class T3BaseProjector(AbsT3Projector):
	"""
	Creates SnapView(s) based on AmpelBuffer(s)
	
	AmpelBuffer attributes are selected according to 'filters' content:
	
	- missing fields will not be included
	  (for example, if 'names' is absent in filters dict,
	  the field 'names' of all snapviews will be None)
	- a "pass-through" behavior occurs when a dict value are None.
	  (for example, if 'names' is set to None, snapviews instances
	  will contain the all the names of the corresponding AmpelBuffer)
	"""

	class ClassModel(StrictModel):
		key: BufferKey
		model: UnitModel

	class FuncModel(StrictModel):
		key: BufferKey
		func: Callable[[Any], Any]

	class FilterOutModel(StrictModel):
		discard: BufferKey


	logger: AmpelLogger

	# Projections can yield empty buffers, especially if the previous 'filter' stage was not used.
	# Ex: # {
	#	'_id': 318144, 'channel': None, 'created': {}, 'journal': (),
	# 	'updated': {}, 'name': ('ZTF17aaabdku',), 'tag': ('ZTF',)
	# }
	# This parameter determines if those empty buffers should be removed.
	# Detection is based on the None value of <buffer>['stock']['channel']
	# and thus requires 'stock' documents to loaded which should be the most frequent case
	remove_empty: bool = True

	# Modify/delete dict keys/values
	field_projectors: List[Union[ClassModel, FuncModel]] = []


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		# List matchers
		self.projectors: Dict[BufferKey, Optional[List[Callable[[Any], Any]]]] = {}

		for fp in self.field_projectors:
			if isinstance(fp, self.ClassModel):
				self.add_class_projector(fp)
			elif isinstance(fp, self.FuncModel):
				self.add_func_projector(fp.key, fp.func)
			elif isinstance(fp, self.FilterOutModel):
				self.projectors[fp.discard] = None

		self.pass_through_keys: Set[BufferKey] = {"stock", "t0", "t1", "t2", "logs", "extra"} - self.projectors.keys()# type: ignore


	def add_class_projector(self, cm: ClassModel, first: bool = False) -> None:

		# Instantiate field projector units (auxiliary units)
		unit = AuxUnitRegister.new_unit(
			model = cm.model,
			sub_type = AbsApplicable,
			logger = self.logger
		)

		self.add_func_projector(cm.key, unit.apply, first=first)


	def add_func_projector(self,
		key: BufferKey, func: Callable, first: bool = False
	) -> None:
		"""
		A matcher rejects/accepts dict instances from a list referenced by the given key.
		For example, a ampel_buffer intance can contain many t2 documents (ampel_buffer dict key "t2").
		The matchers will determine which dict from the list should be kept
		(a typical case would be to exclude dicts based on channel associations)

		:param first: whether field matchers should be put first in the list of matchers for the provided key
		:raise: ValueError if requested field projector unit / configs are invalid
		"""

		if self.logger.verbose > 1:
			self.logger.debug(f"Adding function '{func.__name__}' as field projector for '{key}'")

		# Upate internal set
		self.pass_through_keys -= set([key])

		# New projector for this key
		if key in self.projectors:

			if self.projectors[key] is None:
				raise ValueError(
					f"Cannot setup projector for key '{key}' as the "
					f"null projector was requested for this key"
				)
			# Put new projector first in the list for this key
			if first:
				self.projectors[key].insert(0, func) # type: ignore[union-attr]
			else:
				self.projectors[key].append(func) # type: ignore[union-attr]

		# No field projector was setup for this key yet
		else:
			self.projectors[key] = [func]


	def project(self, ampel_buffer: Iterable[AmpelBuffer]) -> Sequence[AmpelBuffer]:

		# micro optimization
		projectors = self.projectors
		pass_through_keys = self.pass_through_keys

		ret: List[AmpelBuffer] = []

		for abuf in ampel_buffer:

			new_buf = AmpelBuffer(id=abuf["id"])

			for k in pass_through_keys:
				if k in abuf:
					new_buf[k] = abuf[k] # type: ignore[misc]

			# Loop through field projectors defined in config
			for k in projectors.keys():

				# Last condition handles the 'null projector'
				if k in abuf and abuf[k] and projectors[k]:

					new_buf[k] = abuf[k]
					for p in projectors[k]: # type: ignore[union-attr]
						new_buf[k] = p(new_buf[k])
						if not new_buf[k]:
							break

					if self.logger.verbose:

						if isinstance(abuf[k], (list, tuple)):
							self.logger.log(VERBOSE, None,
								extra={'projection': k, 'in': len(abuf[k]), 'out': len(new_buf[k])} # type: ignore
							)
						elif k == 'stock':
							self.logger.log(VERBOSE, None,
								extra={
									'projection': k,
									'journal_in': len(abuf[k]['journal']),     # type: ignore[index]
									'journal_out': len(new_buf[k]['journal'])  # type: ignore[index]
								}
							)

						#for d in [abuf[k]] if k=='stock' else abuf:
						if self.logger.verbose > 1:
							self.logger.debug(None, extra={'projection': k, 'input': abuf[k]}) # type: ignore[str-bytes-safe]
							self.logger.debug(None, extra={'projection': k, 'output': new_buf[k]}) # type: ignore[str-bytes-safe]

			# If the previous stage did not filter properly (or at all), projections can yield empty buffers like this:
			# By default, we remove those
			# Note that it actually requires 'stock' documents to be requested/loaded which should be almost always the case
			if self.remove_empty and 'stock' in new_buf and not new_buf['stock']['channel']: # type: ignore[index]
				if self.logger.verbose > 1:
					self.logger.debug(f"Removing {new_buf['id']} emptied by projection") # type: ignore
				continue

			ret.append(new_buf)

		return ret
