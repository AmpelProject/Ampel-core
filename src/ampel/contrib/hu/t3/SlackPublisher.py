#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t3/SlackPublisher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.03.2018
# Last Modified Date: 17.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsT3Unit import AbsT3Unit
from slackclient import SlackClient
from datetime import datetime, timedelta
from astropy import time

class SlackPublisher(AbsT3Unit):
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

		# Instantiate slack client with auth config originating from the ampel config db
		sc = SlackClient(run_config['auth_token'])

		# Run config
		channel = run_config['channel']
		msg = run_config['msg'] + "\n" if 'msg' in run_config else ""
		dtf = run_config['datetime'] if 'datetime' in run_config else '%d/%m/%Y %H:%M:%S'
		pos = run_config['pos'] if 'pos' in run_config else 'brightest'

		# Will hold the transient ids of 'transients'
		tr_ids = []

		# Loop through provided transients 
		for tran in transients:

			try:
				tr_ids.append(tran.tran_id)

				l = sorted(tran.latest_lightcurve.al_pps_list, key=lambda k: k.content['jd'])

				first_obs_dt = time.Time(l[0].content['jd'], format="jd").datetime
				last_obs_dt  = time.Time(l[-1].content['jd'], format="jd").datetime

				srs = tran.get_science_records(t2_unit_id="SNCOSMO")

				# TODO: improve
				# RUSH CODE TO HAVE SOMETHING READY FOR ZTF CONF
				if type(srs) is list:
					latest_sr = srs[-1]
				else:
					latest_sr = srs

				root_res = latest_sr.get_results() 

				if root_res is None:
					# TODO: better feedback
					self.logger.info("Science record results is None")	
					continue

				# Get latest result from the list. TODO: feedback
				root_res = root_res[-1]

				if not 'results' in root_res or 'fit_acceptable' not in root_res['results']:
					self.logger.info("Science record results missing")	
					
				if root_res['results']['fit_acceptable'] is False:
					self.logger.info("fit_acceptable is False")	

				# Should convert julian date into datetime (Not sure at all it works as intended)
				peak_dt = time.Time(root_res['results']['fit_results']['t0'], format="jd").datetime
				td = timedelta(days=root_res['results']['fit_results']['t0.err'])
				peak_err = td.total_seconds() / (3600 * 24)

				# Post slack message
				sc.api_call(
		   			"chat.postMessage",
		   			channel = channel,
		   			text = (
						# Optional header defined in run_config
						msg + 
						(
							# Main message
							("```Id: %s\nPos (%s): %s\n" +
							 "Inserted: %s\nLast modified: %s\n" + 
							 "First obs: %s\nLast obs: %s\n" +
							 "Fitted peak date: %s +- %s days\nChi2: %s```") %
							(
								tran.tran_id, 
								pos, 
								tran.latest_lightcurve.get_pos(ret=pos),
								tran.created.strftime(dtf),
								tran.modified.strftime(dtf),
								first_obs_dt.strftime(dtf),
								last_obs_dt.strftime(dtf),
								peak_dt.strftime(dtf),
								peak_err,
								root_res['results']['sncosmo_info']['chisq'],
							)
						)
					),
					user = "T3Bot"
		   		)
			# BAD. Last minute caltech demo quick n dirty
			except:
				pass

		self.logger.info(
			"Published following transients to slack channel %s: %s" %
			(channel, tr_ids)
		)
