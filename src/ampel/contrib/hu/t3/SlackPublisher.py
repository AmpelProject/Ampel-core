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
from jdcal import jd2gcal, MJD_0
from datetime import datetime, timedelta

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

		# Instanciate slack client with auth config originating from the ampel config db
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

			tr_ids.append(tran.tran_id)

			# Should convert julian date into datetime. 
			# Not sure at all it works as intended
			conv_tmp = jd2gcal(MJD_0, tran.latest_lightcurve.al_pps_list[0].content['jd'] - 2400000.5)
			first_obs_dt = datetime(*conv_tmp[0:3])+timedelta(days=conv_tmp[3])

			# Should convert julian date into datetime. 
			# Not sure at all it works as intended
			conv_tmp = jd2gcal(MJD_0, tran.latest_lightcurve.al_pps_list[-1].content['jd'] - 2400000.5)
			last_obs_dt = datetime(*conv_tmp[0:3])+timedelta(days=conv_tmp[3])

			# Post slack message
			sc.api_call(
    			"chat.postMessage",
    			channel = channel,
    			text = (
					# Optional header defined in run_config
					msg + 
					(
						# Main message
						"```Id: %s\nPos (%s): %s\nInserted: %s\nLast modified: %s\nFirst obs: %s\nLast obs: %s```" %
						(
							tran.tran_id, 
							pos, 
							tran.latest_lightcurve.get_pos(ret=pos),
							tran.created.strftime(dtf),
							tran.modified.strftime(dtf),
							first_obs_dt.strftime(dtf),
							last_obs_dt.strftime(dtf)
						)
					)
				),
				user = "T3Bot"
    		)

		self.logger.info(
			"Published following transients to slack channel %s: %s" %
			(channel, tr_ids)
		)
