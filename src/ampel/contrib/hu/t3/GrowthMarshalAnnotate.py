#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t3/GrowthMarshalAnnotate.py
# License           : BSD-3-Clause
# Author            : uf <ulrich.feindt@fysik.su.se>, jn <jnordin@physik.hu-berlin.de>
# Date              : 22.03.2018
# Last Modified Date: 22.03.2018
# Last Modified By  : uf <ulrich.feindt@fysik.su.se>, jn <jnordin@physik.hu-berlin.de>

# Assumptions: programidx and sourceid stays the same in the marshal (for some user and program)
# such that they do not need to be verified.
# Assumption for running this T3 is that the marshal sourceid for this channel/program-id is stored in the
# database (i.e. this runs after GrowthMarshalPull)
# For valery, how do we save the sourceid to the database such that the correct sourceid is saved?
#
# Q: How do you get the annotation ID for deleting it?
# Q: It looks like autoannotations are visible for everyone with access to that transient.


from ampel.abstract.AbsT3Unit import AbsT3Unit
from jdcal import jd2gcal, MJD_0
from datetime import datetime, timedelta
import requests


class GrowthMarshalAnnotate(AbsT3Unit):
	"""
	"""

	version = 0.1

	def __init__(self, logger, base_config=None):
		"""
		"""
		self.logger = logger

	def run(self, run_config, transients=None):
		"""
		"""

		self.logger.info("Running with run config %s" % run_config)


		# Run config
		username = run_config['username']
		password = run_config['password']
                programidx = run_config['programidx']  # We might not need this!


		# Will hold the transient ids of 'transients'
		tr_ids = []
                # Will hold a list of dictionaries that will be bulk transmitted
                tr_dicts = []

		# Loop through provided transients 
		for tran in transients:

                        # Load the source ID for this *transient* *AMPEL channel* and *GROWTH program*
                        # Actually, it looks like we do not need the GROWTH program ID, source ID is unique
                        sourceid = tran["GROWTH_sourceid"]
                        
			tr_ids.append(tran.tran_id)

                        
                        

                        
    		

		self.logger.info(
			"Published following transients to slack channel %s: %s" %
			(channel, tr_ids)
		)
