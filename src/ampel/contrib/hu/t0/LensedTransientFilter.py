#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/LensedTransientFilter.py
# License           : BSD-3-Clause
# Author            : m. giomi <matteo.giomi@desy.de>
# Date              : 04.27.2018
# Last Modified Date: 10.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import numpy as np
from extcats import CatalogQuery
from pymongo import MongoClient
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.abstract.AbsAlertFilter import AbsAlertFilter


class LensedTransientFilter(AbsAlertFilter):
	"""
	"""

	# Static version info
	version = 0.1

	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		"""
		
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.on_match_t2_units = on_match_t2_units
		self.min_ndet = run_config['MinNdet']
		self.search_radiuses = {
			'cluslist': run_config['ClusListSearchRadius'],
			'masterlens': run_config['MasterlensSearchRadius'],
			'castleqso': run_config['CaslteQSOSearchRadius']
		}
		
		# init the catalog query objects
		catq_kwargs = {
			'logger': logger, 
			'dbclient': MongoClient(
		    	host = base_config['mongodbHost'], 
				port = base_config['mongodbPort']
			)
		}

		# TODO: add comment
		cluslist_query = CatalogQuery.CatalogQuery(
			"cluslist", ra_key = 'ra_deg', dec_key = 'dec_deg', **catq_kwargs
		)

		# TODO: add comment
		mlens_query = CatalogQuery.CatalogQuery(
			"masterlens", ra_key = 'ra_coord', dec_key = 'dec_coord', **catq_kwargs
		)

		# TODO: add comment
		castle_query = CatalogQuery.CatalogQuery(
			"castleqso", ra_key = 'ra', dec_key = 'dec', **catq_kwargs
		)
		
		# group the catalogs together
		self.catqueries = {
			'cluslist': cluslist_query,
			'masterlens': mlens_query,
			'castleqso': castle_query
		}

		# Feedback
		for cat, rs in self.search_radiuses.items():
			self.logger.info("Catalog: %s --> Search radius: %.2e arcsec"%(cat, rs))
		

	def apply(self, alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either 
			* return self.on_match_default_flags
			* return a custom combination of T2 unit names
		"""

		# cut on the number of previous detections
		if len(alert.pps) < self.min_ndet:
			return None
		
		# now match with the catalogs
		mean_ra, mean_dec = np.mean(alert.get_ntuples(["ra", "dec"]), axis = 0)
		for cat, catquery in self.catqueries.items():
			rs = self.search_radiuses[cat]
			if catquery.binaryserach(mean_ra, mean_dec, rs):
				self.logger.debug("searching matches in %s within %.2f arcsec"%(cat, rs))
				return self.on_match_t2_units 

		return None

