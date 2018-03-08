#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/examples/t0/ExampleFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsAlertFilter import AbsAlertFilter

class ExampleFilter(AbsAlertFilter):
	"""
	REQUIREMENTS:
	-------------

	A T0 filter class must (otherwise exception will be throwed):
	* inherit the abstract parent class 'AbsAlertFilter'
	* define a class variable named 'version' with a float value
	* implement the following two functions:
		-> __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None)
		-> apply(self, ampel_alert)
	"""

	# Static version info
	version = 0.2


	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		Mandatory implementation.
		Parameter 'd' is a dict instance loaded from the ampel config. 
		It means d can contain any parameter you define 
		for your channel in 'alertFilter.parameters'
		"""

		# Instance variable holding reference to provider logger 
		self.logger = logger

		# Logging example
		self.logger.info("Please use this logger object for logging purposes")
		self.logger.debug("The log entries emitted by this logger will be stored into the Ampel DB")
		self.logger.debug("This logger is to be used 'as is', please don't change anything :)")

		# TODO: explain
		self.on_match_t2_units = on_match_t2_units

		# Example: 'magpsf' (see the jupyter notebook "Understanding T0 Filters")
		self.filter_field = base_config['field']

		# Example: 18 (see the jupyter notebook "Understanding T0 Filters")
		self.threshold = run_config['threshold']


	def apply(self, ampel_alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either 
			* return self.on_match_t2_units
			* return a custom combination of ampel.flags.T2UnitIds

		In this example filter, any measurement of a transient 
		brighter than a fixed magnitude threshold will result 
		in this transient being inserted into ampel.
		"""

		# One way of filtering alerts based on fixed mag threshold
		# (for other means of achiving the same results, please 
		# see the jupyter notebook "Understanding T0 Filters")
		for pp in ampel_alert.get_photopoints():

			# Example: 
			# self.filter_field: "magpsf"
			# self.threshold: 18
			if pp[self.filter_field] < self.threshold :
				return self.on_match_t2_units

		return None
