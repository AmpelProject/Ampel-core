#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/AmpelAlertPlotter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.01.2018
# Last Modified Date: 23.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import matplotlib.pyplot as plt

class AmpelAlertPlotter:
	
	@staticmethod	
	def plot(ampel_alert, p1, p2):
		"""
		Parameters:
		ampel_alert: instance of ampel.pipeline.t0.AmpelAlert 
		p1: parameter (ex: 'obs_date')
		p2: parameter (ex: 'magpsf')
		"""
		plt.scatter(*zip(*ampel_alert.get_tuples(p1, p2)))
		plt.xlabel(p1)
		plt.ylabel(p2)
		plt.grid(True)
		plt.show()
