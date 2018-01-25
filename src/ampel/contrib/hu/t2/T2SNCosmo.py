#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Module.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 13.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.base.abstract.AmpelABC import AmpelABC, abstractmethod
from ampel.base.abstract.AbstractT2Module import AbstractT2Module

class T2SNCosmo(AbstractT2Module):

	"""
	"""
	
	def __init__(self, base_parameters):
		"""
			base_parameters: dict instanciated based on ampel config
		"""
		self.d_parameters = base_parameters


	def run(self, light_curve, custom_parameters):
		""" 
			light_curve: "ampel.pipeline." instance
			param_id: string value that match dict key in 
			ampel config section associated with this module
		"""
		
		return {'test': 1}	
