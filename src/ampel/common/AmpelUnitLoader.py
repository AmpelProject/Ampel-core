#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/common/AmpelUnitLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.10.2018
# Last Modified Date: 02.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources
from ampel.base.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.base.abstract.AbsT3Unit import AbsT3Unit
from ampel.config.AmpelConfig import AmpelConfig

class AmpelUnitLoader:
	"""
	Class used to load/retrieve ampel unit classes (t0 filters, T2 or T3 units).
	It holds a static dict instance referencing loaded *classes* (not instances) 
	in order to avoid multiple reloading of classes shared 
	among several different tasks/jobs
	"""

	UnitClasses = {
		0: {},
		2: {},
		3: {}
	}

	_AbsClasses = {
		0: AbsAlertFilter,
		2: AbsT2Unit,
		3: AbsT3Unit
	}


	@classmethod
	def get_class(cls, tier, unit_name, logger=None, raise_exc=False):
		"""
		Retrieve and return unit class registered at entry point 
		ampel.pipeline.t%is.units where %i can be 0, 2 or 3

		:param int tier: 0, 2 or 3
		:param str unit_name: unit name 
		:param Logger logger: optional logger from python module 'logging'
		:raises: if raise_exc is True, errors are raises:
		- NameError if unit is not found
		- ValueError if unit parent is not AbsAlertFilter or AbsT2Unit or AbsT3Unit
		:returns: ampel unit class or None
		"""

		# return saved class if unit class was already loaded
		if unit_name in cls.UnitClasses[tier]:
			return cls.UnitClasses[tier][unit_name]
		
		if logger:
			logger.info("Loading T%i unit: %s" % (tier, unit_name))

		# Load resource
		resource = next(
			pkg_resources.iter_entry_points(
				'ampel.pipeline.t%s.units' % tier, 
				unit_name
			),
			None
		)

		if resource is None:
			if logger:
				logger.error("Unknown T%s unit: %s" % (tier, unit_name))
			if raise_exc:
				raise NameError("Unknown T%s unit: %s" % (tier, unit_name))
			return None

		# Get class
		UnitClass = resource.resolve()

		# Check parent
		if not issubclass(UnitClass, cls._AbsClasses[tier]):

			if logger:
				logger.error(
					"T{} unit {} from {} is not a subclass of AbsT3Unit".format(
						tier, UnitClass.__name__, resource.dist
					)
				)

			if raise_exc:
				raise ValueError(
					"T{} unit {} from {} is not a subclass of AbsT3Unit".format(
						tier, UnitClass.__name__, resource.dist
					)
				)

			return None

		# Save loaded class to static dict
		cls.UnitClasses[tier][unit_name] = UnitClass

		return UnitClass

	@classmethod
	def reset(cls):
		for value in cls.UnitClasses.values():
			value.clear()

	@classmethod
	def get_resources(cls, UnitClass):
		""" 
		Gather resources associated with provided class
		"""
		return {
			k: AmpelConfig.get_config('resources.{}'.format(k)) 
			for k in getattr(UnitClass, 'resources', [])
		}
