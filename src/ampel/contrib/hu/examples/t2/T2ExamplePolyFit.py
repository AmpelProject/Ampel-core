#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/examples/t2/T2ExamplePolyFit.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 28.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod
from ampel.abstract.AbstractT2Runnable import AbstractT2Runnable
import numpy

class T2ExamplePolyFit(AbstractT2Runnable):
	"""
	Example of what the base structure of T2 class should look like.
	The following two functions *must* be implemented (otherwise exception will be throwed):
	- run(self, light_curve, run_parameters)
	- get_version(self):
	The function set_base_parameters(self, dict_instance) if implemented, 
	will be called automatically by Ampel after class instanciation

	The ampel config entry for this T2Runnable could look like this:
    'T2' : {
        'runnables' : {
            'POLYFIT' : {
				'RunnableId': 'ampel.flags.T2RunnableIds.POLYFIT',
				'classFullPath': 'ampel.contrib.hu.examples.t2.T2ExamplePolyFit',
				'base_parameters': {
                	'fit_function' : 'polyfit',
				},
                'run_parameters' : {
                    'default' : {
                        'degree' : 3
                    },
                    'advanced' : {
                        'degree' : 5
                    }
                }
            }
        }
    }
	"""

	def set_base_parameters(self, base_parameters):
		"""
		Optional method. If implemented, Ampel will call 
		this method automatically after class instanciation

		base_parameters: dict loaded from ampel config section:
		T2->runnables->POLYFIT->base_parameters
		"""
		self.fit_function = getattr(numpy, base_parameters['fit_function'])


	def get_version(self):
		return 0.1


	def run(self, light_curve, run_parameters):
		""" 
		light_curve: "ampel.base.LightCurve" instance. See the LightCurve docstring for more info.
		run_parameters: dict containing run parameters defined in ampel config section:
		T2->runnables->POLYFIT->run_parameters->parameterId
		whereby the parameterId value is defined in the associated t2 document.
		In the case of POLYFIT, parameterId would be either 'default' or 'advanced'.
		A given channel (say CHANNEL_HU_SNIA) would use say the parameterId 'default'
		whereas another channel (say CHANNEL_OKC_SNIIP) would use the parameterId 'advanced'
		"""

		x = light_curve.get_values("obs_date")
		y = light_curve.get_values("mag")
		p = self.fit_function(x, y, run_parameters['degree'])
		chi_squared = numpy.sum((numpy.polyval(p, x) - y) ** 2)

		return {
			"polyfit": list(p),
			"chi2": numpy.sum((numpy.polyval(p, x) - y) ** 2)
		}
