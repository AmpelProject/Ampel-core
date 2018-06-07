#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/SEDmTargetFilter.py
# License           : BSD-3-Clause
# Author            : m. giomi <matteo.giomi@desy.de>
# Date              : 06.06.2018
# Last Modified Date: 06.06.2018
# Last Modified By  : m. giomi <matteo.giomi@desy.de>

import numpy as np
import logging
from catsHTM import cone_search
from ampel.pipeline.common.expandvars import expandvars

class DecentFilter():
	"""
		Filter to make a decent selection of candidates. Partially based on the 
		Redshift completeness prorgam filter on the marshal.
		Apply cuts on:
			* magniture
			* numper of detections
			* real bogus
			* sgscore of closest PS1 if close enough
			* presence of brihgt starlike PS1 neightbours
			* image parameters such as fwhm, elongation, bad pixels, and the
			difference between PSF and aperture magnitude.
	"""

	# Static version info
	version = 1.0

	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		"""
		if run_config is None or len(run_config) == 0:
			raise ValueError("Please check you run configuration")

		if base_config is None or len(base_config) == 0:
			raise ValueError("Please check you base configuration")

		self.on_match_t2_units = on_match_t2_units
		self.logger = logger if logger is not None else logging.getLogger()
#		self.logger.setLevel(logging.DEBUG)
		
		# Robustness and feedback
		config_params = (
			'magTh',
			'realBogusTh',
			'minNDet',
			'clostestPS1_MinDist',
			'clostestPS1_MinDist',
			'nearbyPS1_SgTh',
			'nearbyPS1_MinDist',
			'neabyPS1_rMagTh',
			'GaiaSearchRadius',
			'maxFwhm',
			'maxElong',
			'maxNBadPixels',
			'maxMagDiff'
			)
		for el in config_params:
			if el not in run_config:
				raise ValueError("Parameter %s missing, please check your channel config" % el)
			if run_config[el] is None:
				raise ValueError("Parameter %s is None, please check your channel config" % el)
			self.logger.info("Using %s=%s" % (el, run_config[el]))

		# parse filter parameters
		self.mag_th					= run_config['magTh']
		self.rb_th					= run_config['realBogusTh']
		self.min_ndet				= run_config['minNDet']
		
		# if there is a closeby PS1 source with relatively high sgscore, the alert is rejected.
		self.closesPS1_sg_th		= run_config['closestPS1_SgTh']	# valery is going to appreaciate the syntax
		self.closestPS1_min_dist	= run_config['clostestPS1_MinDist']
		
		# if there is any neaby PS1 star with decent sgscore and bright enough
		# the alert is rejected as well.
		self.nearbyPS1_sg_th		= run_config['nearbyPS1_SgTh']
		self.nearbyPS1_min_dist		= run_config['nearbyPS1_MinDist']
		self.nearbyPS1_rmag_th		= run_config['neabyPS1_rMagTh']
		
		# image and pixel cuts
		self.max_nbad				= run_config['maxNBadPixels']
		self.max_fwhm 				= run_config['maxFwhm']
		self.max_elong				= run_config['maxElong']
		self.max_magdiff			= run_config['maxMagDiff']

		# then we also search in GAIA
		self.search_radius 			= run_config['GaiaSearchRadius']
		self.catshtm_path 			= base_config['CatsHtmPath']
		self.logger.info("using catsHTM files in %s"%self.catshtm_path)
		self.logger.info(
			"Serach radius for vetoing sources in GAIA DR2: %.2f arcsec" % 
			self.search_radius
		)

		# Robustness
		self.keys_to_check = (
			'nbad', 'fwhm', 'elong', 'isdiffpos',
			'sgscore1', 'distpsnr1', 'srmag1',
			'sgscore2', 'distpsnr2', 'srmag2',
			'sgscore3', 'distpsnr3', 'srmag3',
			'rb', 'magpsf', 'magdiff'
			)


	def apply(self, alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either return
			* self.on_match_t2_units
			* or a custom combination of T2 unit names
		"""
		
		
		# get the lates photo-point and check that the relevant keys are present
		latest = alert.pps[0]
		for el in self.keys_to_check:
			if el not in latest:
				self.logger.debug("rejected: '%s' missing" % el)
				return None
			if latest[el] is None:
				self.logger.debug("rejected: '%s' is None" % el)
				return None
		
		# --------------------------------------------------------------------- #
		#																		#
		#						CUTS ON IMAGE PROPERTIES						#
		#																		#
		# --------------------------------------------------------------------- #
		
		# check on the number of bad pixels.
		if latest['nbad'] > self.max_nbad:
			self.logger.debug(
				"rejected: found %d bad pixels around transient (max allowed: %d)."%
				(latest['nbad'], self.max_nbad)
			)
			return None
		
		# cut on image fwhm
		if latest['fwhm'] > self.max_fwhm:
			self.logger.debug(
				"rejected: fwhm %.2f above threshod (%.2f)" % 
				(latest['fwhm'], self.max_fwhm)
			)
			return None
		
		# now on elongation
		if latest['elong'] > self.max_elong:
			self.logger.debug(
				"rejected: elongation %.2f above threshod (%.2f)" % 
				(latest['elong'], self.max_elong)
			)
			return None
		
		# on the difference between psf and aperture magnitudes
		if abs(latest['magdiff']) > self.max_magdiff:
			self.logger.debug(
				"rejected: difference between PSF and apert. mag %.2f above threshod (%.2f)" % 
				(latest['magdiff'], self.max_magdiff)
			)
			return None
		
		# cut on RB (1 is real, 0 is bogus)
		if latest['rb'] < self.rb_th:
			self.logger.debug("rejected: RB score %.2f below threshod (%.2f)" %
				(latest['rb'], self.rb_th))
			return None
		
		# check if it a positive subtraction
		if not (latest['isdiffpos'] and (latest['isdiffpos'] == 't' or latest['isdiffpos'] == '1')):
			self.logger.debug("rejected: 'isdiffpos' is %s", latest['isdiffpos'])
			return None
		
		# --------------------------------------------------------------------- #
		#																		#
		#						CUTS ON ASTRO PROPERTIES						#
		#																		#
		# --------------------------------------------------------------------- #
		
		# check if the alert is bright enough
		if latest['magpsf'] > self.mag_th:
			self.logger.debug(
				"rejected: magnitude %.2f above threshod (%.2f)" % 
				(latest['magpsf'], self.mag_th)
			)
			return None
		
		# check that you have multiple detections
		npp = len(alert.pps)
		if npp < self.min_ndet:
			self.logger.debug(
				"rejected: only %d photopoints in alert (minimum required %d)" % 
				(npp, self.min_ndet)
			)
			return None
		
		# if there is a PS1 source very close by, cut on it's SG score
		if (latest['sgscore1'] and latest['distpsnr1'] and 
			latest['sgscore1'] > self.closesPS1_sg_th and 
			latest['distpsnr1'] < self.closestPS1_min_dist):
			self.logger.debug(
				"rejected: closest PS1 cp at %.2f arcsec with and sgscore of %.2f" %
				(latest['distpsnr1'], latest['sgscore1']))
			return None
		
		# check that there is no bright PS1 source nearby. 
		conditions = []
		for ips1 in range(1, 3):
			distps = latest['distpsnr%d'%ips1]
			srmag = latest['srmag%d'%ips1]
			sgscore = latest['sgscore%d'%ips1]
			isbrightstar = 	(
								( distps and distps < self.nearbyPS1_min_dist) and
								( srmag and srmag > 0 and srmag < self.nearbyPS1_rmag_th) and
								( sgscore and sgscore > self.nearbyPS1_sg_th)
							)
			if isbrightstar:
				self.logger.debug(
					"rejected: nearby PS1 source nr. %d is likely a bright star. dist: %.2f, rmag: %.2f, sg: %.2f"%
						(ips1, distps, srmag, sgscore))
				return None
		
		# search within GAIA sources. 
		# some files are corrupted, we have to catch the exception
		try:
			srcs, colnames, colunits = cone_search(
											'GAIADR2',
											latest['ra'],
											latest['dec'],
											self.search_radius,
											catalogs_dir=self.catshtm_path)
			if len(srcs) > 0:
				self.logger.debug(
					"rejected: within %.2f arcsec from a GAIA DR2 source" % 
					(self.search_radius)
				)
				return None
		except OSError as ose:
			self.logger.debug("rejected: OSError from catsHTM %s"%str(ose))
			return None
		
		# contratulations alert, you made it! welcome in the Ampel database!
		return self.on_match_t2_units
