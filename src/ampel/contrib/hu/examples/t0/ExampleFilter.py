#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/examples/t0/ExampleFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 27.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter

class ExampleFilter(AbstractTransientFilter):
	"""
		Your filter must inherit the abstract parent class 'AbstractTransientFilter'
		The following three methods *must* be implemented:
			-> get_version(self)
			-> set_filter_parameters(self, d)
			-> apply(self, ampel_alert)
		The instance variable self.logger (inherited from the parent class) is ready-to-use.
	"""

	# Static version info
	version = 0.2


	def __init__(self):
		"""
		Your constructor (optional)
		"""
		self.logger.info("Please use this logger object for logging purposes")
		self.logger.debug("The log entries emitted by this logger will be stored into the Ampel DB")
		self.logger.debug("This logger is to be used 'as is', please don't change anything :)")

	
	def get_version(self):
		"""
		Mandatory implementation.
		"""
		return ExampleFilter.version


	def set_filter_parameters(self, d):
		"""
		Mandatory implementation.
		This method is called automatically before alert processing.
		Parameter 'd' is a dict instance loaded from the ampel config. 
		It means d can contain any parameter you define 
		for your channel in 'alertFilter.parameters'
		"""
		self.threshold = d['threshold']
		self.my_parameter = d['my_parameter']

		# You could instance here a dictionary later used in the method apply 
		# (see the jupyter notebook "Understanding T0 Filters")
		self.my_filter = {'mag': self.threshold, 'op': '<'}


	def apply(self, ampel_alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either 
			* return self.on_match_default_flags
			* return a custom combination of T2RunnableIds

		In this example filter, any measurement of a transient 
		brighter than a fixed magnitude threshold will result 
		in this transient being inserted into ampel.
		"""

		# One way of filtering alerts based on fixed mag threshold
		# (for other means of achiving the same results, please 
		# see the jupyter notebook "Understanding T0 Filters")
		for pp in ampel_alert.get_photopoints():
			if pp['magpsf'] < self.threshold:
				return self.on_match_default_flags

		return None
