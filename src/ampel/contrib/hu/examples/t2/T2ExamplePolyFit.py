#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/examples/t2/T2ExamplePolyFit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.02.2018
# Last Modified Date: 07.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod
from ampel.abstract.AbsT2Unit import AbsT2Unit
from ampel.flags.T2RunStates import T2RunStates
import numpy

class T2ExamplePolyFit(AbsT2Unit):
	"""
	Example of what a simple T2 class should look like.

	REQUIREMENTS:
	-------------

	A T2 class must (otherwise exception will be throwed):
	* inherit the abstract parent class 'AbsT2Unit'
	* define a class variable named 'version' with a float value
	* implement the following two functions
		-> init(self, logger, base_config=None) 
		-> run(self, light_curve, run_config=None)


	ASSOCIATED AMPEL CONFIG EXAMPLE:
	--------------------------------

	The ampel config entries for this T2Unit could look like this:

	Collection 't2_units':
	{
	    "_id" : "POLYFIT",
	    "classFullPath" : "ampel.contrib.hu.examples.t2.T2ExamplePolyFit",
	    "author" : "ztf-software@desy.de",
	    "version" : 1.0,
	    "baseConfig" : {
	        "fitFunction" : "polyfit"
	    }
	}

	Collection 't2_run_config':

	{
	    "_id" : "POLYFIT_default",
		"author" : "ampel@physik.hu-berlin.de",
		"version" : 1.0,
		"lastChange" : "28.02.2018",
		"runConfig" : {
			"degree" : 3
		}
	},
	{
	    "_id" : "POLYFIT_advanced",
		"author" : "ampel@physik.hu-berlin.de",
		"version" : 1.0,
		"lastChange" : "26.02.2018",
		"runConfig" : {
			"degree" : 5
		}
	}
	"""

	version = 1.0

	def __init__(self, logger, base_config):
		"""
		'logger': instance of logging.Logger (std python module 'logging')
			-> example usage: logger.info("this is a log message")

		'base_config': optional dict loaded from ampel config section: 
			t2_units->POLYFIT->baseConfig
		"""
	
		# Save the logger as instance variable
		self.logger = logger

		# Check if base configuration was set properly
		if base_config is None:
			raise ValueError("baseConfig parameter 'fitFunction' is missing")

		# Define the fit_function to be used later in method run(...)
		self.fit_function = getattr(numpy, base_config['fitFunction'])


	def run(self, light_curve, run_config):
		""" 
		'light_curve': instance of ampel.base.LightCurve. See LightCurve docstring for more info.

		'run_config': dict instance containing run parameters defined in ampel config section:
			t2_run_config->POLYFIT_[run_config_id]->runConfig 
			whereby the run_config_id value is defined in the associated t2 document.
			In the case of POLYFIT, run_config_id would be either 'default' or 'advanced'.
			A given channel (say HU_SN_IA) could use the runConfig 'default' whereas 
			another channel (say OKC_SNIIP) could use the runConfig 'advanced'

		This method must return either:
			* A dict instance containing the values to be saved into the DB
				-> IMPORTANT: the dict *must* be BSON serializable, that is:
					import bson
					bson.BSON.encode(<dict instance to be returned>)
				must not throw a InvalidDocument Exception
			* One of these T2RunStates flag member:
				MISSING_INFO:  reserved for a future ampel extension where 
							   T2s results could depend on each other
				BAD_CONFIG:	   Typically when run_config is not set properly
				ERROR:		   Generic error
				EXCEPTION:     An exception occured
		"""

		if run_config is None or 'degree' not in run_config:
			self.logger.error("Run config parameter 'degree' is missing")
			return T2RunStates.BAD_CONFIG

		try:
			x = light_curve.get_values("obs_date")
			y = light_curve.get_values("mag")
			p = self.fit_function(x, y, run_config['degree'])
			chi_squared = numpy.sum((numpy.polyval(p, x) - y) ** 2)

			self.logger.info("Please use 'self.logger' for logging")
			self.logger.debug("By doing so, log entries will be automatically recorded into the database")

			return {
				"polyfit": list(p),
				"chi2": numpy.sum((numpy.polyval(p, x) - y) ** 2)
			}

		except:
			self.logger.error("An exception occured", exc_info=1)
			return T2RunStates.EXCEPTION
