#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t2/T2SNCosmo.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 26.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod
from ampel.abstract.AbstractT2Worker import AbstractT2Worker

class T2SNCosmo(AbstractT2Worker):
	"""
	"""

	def set_base_parameters(self, base_parameters):
		"""
			base_parameters: dict instanciated based on ampel config
		"""
		self.base_parameters = base_parameters


	def run(self, light_curve, run_parameters):
		""" 
			light_curve: "ampel.base.LightCurve" instance
			run_parameters: dict instance of containing custom run parameters 
			defined in the ampel config section associated with this module
		"""

		return {}	
