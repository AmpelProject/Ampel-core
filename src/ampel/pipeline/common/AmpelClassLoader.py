#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelClassLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.10.2018
# Last Modified Date: 02.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources
from ampel.base.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.base.abstract.AbsT3Unit import AbsT3Unit

class AmpelClassLoader:
	"""
	Class used to load/retrieve ampel unit classes (t0 filters, T2 or T3 units).
	It holds a static dict instance referencing loaded *classes* (not instances) 
	in order to avoid multiple reloading of classes shared 
	among several different tasks/jobs
	"""

	Classes = {
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
	def get_unit(cls, tier, unit_name, logger=None):
		"""
		Retrieve and return unit class registered at entry point 
		ampel.pipeline.t%is.units where %i can be 0, 2 or 3

		:param int tier: 0, 2 or 3
		:param str unit_name: unit name 
		:param Logger logger: logger from python module 'logging'
		:returns: ampel unit class
		"""

		if unit_name in cls.Classes[tier]:
			return cls.Classes[tier][unit_name]
		
		if logger:
			logger.info("Loading T%i unit: %s" % (tier, unit_name))

		resource = next(
			pkg_resources.iter_entry_points(
				'ampel.pipeline.t%i.units' % tier, 
				unit_name
			),
			None
		)

		if resource is None:
			raise ValueError("Unknown T%s unit: %s" % (tier, unit_name))

		klass = resource.resolve()
		if not issubclass(klass, cls._AbsClasses[tier]):
			raise TypeError(
				"T{} unit {} from {} is not a subclass of AbsT3Unit".format(
					tier, klass.__name__, resource.dist
				)
			)

		# Save loaded class to static dict
		cls.Classes[tier][unit_name] = klass

		return klass
