#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : src/ampel/contrib/hu/t0/SEDmTargetFilter.py
# Author            : m. giomi <matteo.giomi@desy.de>
# Date              : 04.27.2018
# Last Modified Date: 04.27.2018
# Last Modified By  : mgiomi


import numpy as np
import logging
from extcats import CatalogQuery
from pymongo import MongoClient


class SEDmTargetFilter():
	"""
	Filter to select SEDm targets for the RFC program. This filters
	selects non-star candidates brighter than 19 mag. To exclude the
	stars, both SGScore and catalog matching (using a subset of bright 
	Gaia sources) is used.
	"""

	# Static version info
	version = 1.0

	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		"""
		if run_config is None or len(run_config) == 0:
			raise ValueError("Please check you run configurtion")

		if base_config is None or len(base_config) == 0:
			raise ValueError("Please check you base configurtion")

		self.on_match_t2_units = on_match_t2_units
		self.logger = logger if logger is not None else logging.getLogger()

		# catalog matching 'technical' stuff (db host, port, ecc) in base_config
		# the more astrophysical stuff in run_config
		catq_client = MongoClient(
		    host = base_config['mongodbHost'], 
			port = base_config['mongodbPort']
		)

		self.mag_th			= run_config['MagTh']
		self.sg_th			= run_config['SGscoreTh']
		self.rb_th			= run_config['RealBogusTh']
		self.search_radius 	= run_config['m13GaiaSearchRadius']

		# init the catalog query object for the star catalog
		catq_kwargs = {'logger': logger, 'dbclient': catq_client}
		self.gaia_query = CatalogQuery.CatalogQuery(
			"gaia_dr1_m13", ra_key='ra', dec_key='dec',
			logger=logger, dbclient=catq_client
		)

		self.logger.info(
			"Serach radius for vetoing sources in gaia_dr1_m13: %.2f arcsec" % 
			self.search_radius
		)


	def apply(self, alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either return
			* self.on_match_t2_units
			* or a custom combination of T2 unit names
		"""

		# ---- cuts on magnitude, RB, and sg-score of the last pp ---- #

		# get the lates photo-point
		latest = alert.pps[0]

		# cut on RB (1 is real, 0 is bogus)
		if latest['rb'] < self.rb_th:
			self.logger.debug(
				"rejected: RB score %.2f below threshod (%.2f)" %
				(latest['rb'], self.rb_th)
			)
			return None

		# then on star-galaxy separation (1 for star, 0 for galaxy)
		if latest['sgscore'] > self.sg_th:
			self.logger.debug(
				"rejected: SG score %.2f above threshod (%.2f)" %
				(latest['sgscore'], self.sg_th)
			)
			return None

		# then on the magnitude
		if latest['magpsf'] > self.mag_th:
			self.logger.debug(
				"rejected: magnitude %.2f above threshod (%.2f)" % 
				(latest['magpsf'], self.mag_th)
			)
			return None


		# ---- finally search within bright GAIA sources and reject if matching ---- #

#		mean_ra, mean_dec = np.mean(alert.get_ntuples(["ra", "dec"]), axis = 0)
#		found = self.gaia_query.binarysearch(mean_ra, mean_dec, self.search_radius)
		if self.gaia_query.binaryserach(latest['ra'], latest['dec'], self.search_radius):
			self.logger.debug(
				"rejected: within %.2f arcsec from a brigt gaia source" % 
				(self.search_radius)
			)
			return None

		return self.on_match_t2_units
