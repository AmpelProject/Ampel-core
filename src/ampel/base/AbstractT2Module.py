#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Module.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 13.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.base.AmpelABC import AmpelABC, abstractmethod

class AbstractT2Module(metaclass=AmpelABC):

	"""
	"""
	
	def __init__(self, params=dict()):
		self.params = params

	@abstractmethod
	def run(self, ampel_transient, param_id):
		""" 
		"""
		
		
