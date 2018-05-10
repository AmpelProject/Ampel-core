#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/ztfbh/t0/NuclearFilter.py
# License           : BSD-3-Clause
# Author            : sjoertvv <sjoert@umd.edu>
# Date              : 26.02.2018
# Last Modified Date: 10.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import numpy as np
from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.pipeline.logging.LoggingUtils import LoggingUtils


class TFilter(AbsAlertFilter):
	"""
	"""

	# Static version info
	version = 0.2


	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.on_match_t2_units = on_match_t2_units

		# the max flare-ref distance (distnr) to be accepted, we try mulitple ways of summing distnr for multiple detection
		self.maxDeltaRad = base_config['MaxDeltaRad']

		# not used yet
		# self.maxDeltaRadLate 	= d['MaxDeltaRadLate'] 	

		# star-galaxy scaore in PS1 (this seems to be either zero or one)
		self.minSgscore = base_config['MinSgscore']		

		# max bad pixels, applied before mover cuts
		self.maxNbad = base_config['MaxNbad']			

		# remove movers with min time distance in days between detections that pass default filter
		self.minDeltaJD	= base_config['MinDeltaJD'] 		

		# option to require we have (at least one epoch with) two detections within a night (or even shorter timescale)
		self.maxDeltaJD	= base_config['MaxDeltaJD']		

		# min RealBogus score of *any* observation
		self.minRealBogusScore = base_config['MinRealBogusScore']

		# bright star removal: min PS1 r-band mag 
		self.brightPS1RMag = base_config['BrightPS1RMag']	

		# min distance to nearest PS1 source (useful for removing bright stars and ghostly things)
		self.minDistPS1source = base_config['MinDistPS1source']	

		# max distance for checking nearby bright stars in PS1
		self.maxDistPS1source = base_config['MaxDistPS1source']	

		# bright star removal: used for both ZTF filters
		self.brightRefMag = base_config['BrightRefMag'] 	

		# use only most recent detection (attempt to simulate real time)
		self.LastOnly = base_config['LastOnly'] 		


		# Instance dictionaries later used in method apply 
		# remove upper limits
		isdetect_flt = {
			'attribute':'candid', 
			'operator': 'is not',
			'value': None
		}	
		
		# not too many bad pixels
		nbad_flt = {
			'attribute':'nbad', 
			'operator': '<=',
			'value':self.maxNbad
		}

		# is not ref-science
		isdiff_flt1 = {
			'attribute':'isdiffpos',
			'operator': '!=',
			'value':'0'
		}

		# is not ref-science (again!)
		isdiff_flt2 = {
			'attribute':'isdiffpos',
			'operator': '!=',
			'value':'f'
		}

		# has host galaxy detected (removes orphans)
		distnr_flt = {
			'attribute':'distnr',
			'operator': '>',
			'value': 0
		}

		self._default_filters = [isdetect_flt, nbad_flt, distnr_flt, isdiff_flt1, isdiff_flt2]

		self.logger.info("NuclearFilter initialized")


	def apply(self, alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either 
			* return self.on_match_t2_units
			* return a custom combination of T2 unit names

		Make a selection on:
		- the distance between the transient and host in reference image
		- the Real/Bogus sore
		- the distance to a bright star 
		"""		

		# first check we have an extended source (note this can remove flares from faint galaxies that missclassified in PS1)
		# these will have to be dealt with in the orphan/faint filter	

		sgscore = alert.get_values("sgscore1")
		if len(sgscore) == 1:

			distpsnr1 = alert.pps[0]["distpsnr1"]
			sgscore = alert.pps[0]["sgscore1"]
			srmag1 = alert.pps[0]["srmag1"]
			sgmag1 = alert.pps[0]["sgmag1"]

			#sgscore2, sgscore3 = alert.pps[0]["sgscore2"], alert.pps[0]["sgscore3"]
			distpsnr2, distpsnr3 = alert.pps[0]["distpsnr2"], alert.pps[0]["distpsnr3"]
			srmag2, srmag3 = alert.pps[0]["srmag2"],alert.pps[0]["srmag3"]
			sgmag2, sgmag3 = alert.pps[0]["sgmag2"],alert.pps[0]["sgmag3"]

		# exception for older (pre v1.8) schema	
		else:
			sgscore = sgscore = alert.pps[0]["sgscore"]
			distpsnr1 = -999 #alert.get_values("distpsnr")[0]
			srmag1 = alert.pps[0]["srmag"]
			sgmag1 = alert.pps[0]["sgmag"]
			srmag2 = None
			
		if sgscore is None:		
			self.why = "sgscore=None"
			self.logger.info(self.why)
			return None

		if sgscore > self.minSgscore:				
				self.why = "sgscore={0:0.2f}, which is > {1:0.2f}".format(sgscore, self.minSgscore)
				self.logger.info(self.why)
				return None

		if srmag1 is None:
			self.why = "sr mag is None"
			self.logger.info(self.why)
			return None

		if (srmag1 < 0) or (sgmag1 < 0):
			self.why = "1st PS1 match is faulty: sgmag={0:0.2f} srmag={1:0.2f} (dist={2:0.2f})".format(sgmag1, srmag1, distpsnr1)
			self.logger.info(self.why)
			return None

		if srmag1 < self.brightPS1RMag:
			self.why = "1st PS1 match srmag={0:0.2f}, which is < {1:0.2f} (dist={2:0.2f} arcsec)".format(srmag1, self.brightPS1RMag, distpsnr1)
			self.logger.info(self.why)
			return None

		# if we have the new schema, also check for nearby bright stars 
		if srmag2 is not None:
			if (abs(srmag2) < self.brightPS1RMag) and (abs(distpsnr2) < self.maxDistPS1source):
				self.why = "2nd PS1 match srmag={0:0.2f}, which is < {1:0.2f} (dist={2:0.2f})".format(srmag2, self.brightPS1RMag, distpsnr2)
				self.logger.info(self.why)
				return None

			if (abs(srmag3) < self.brightPS1RMag) and (abs(distpsnr3) < self.maxDistPS1source):
				self.why = "3rd  PS1 match r={0:0.2f}, which is < {1:0.2f} (dist={2:0.2f})".format(srmag3, self.brightPS1RMag, distpsnr3)
				self.logger.info(self.why)
				return None

			# important: also check that the nearest PS1 source is not too far 	
			if abs(distpsnr1) > self.minDistPS1source:
					self.why = "distance to 1st PS1 match is {0:0.2f}, which is > {1:0.2f}".format(distpsnr1, self.minDistPS1source)
					self.logger.info(self.why)
					return None


			# don't use the code below because it will remove sources next to objects 
			# that were detected in just one pan-starrs band and thus have srmag=-999
			# 
			# if ((srmag2<0) or (sgmag2<0)) and (abs(distpsnr2)< self.maxDistPS1source):
			# 	self.why = "2nd PS1 match saturated(?) sgmag={0:0.2f} srmag={1:0.2f} (dist={2:0.2f})".format(sgmag2, srmag2, distpsnr2)
			# 	self.logger.info(self.why)
			# 	return None

			# if ((srmag3<0) or (sgmag3<0)) and (abs(distpsnr3)< self.maxDistPS1source):
			# 	self.why = "3rd PS1 match saturated(?) sgmag={0:0.2f} srmag={1:0.2f} (dist={2:0.2f})".format(sgmag3, srmag3, distpsnr3)
			# 	self.logger.info(self.why)
			# 	return None

		# get RealBogus scores for observations, check number of bad pixels
		tuptup = alert.get_ntuples(["rb","jd", "magnr", "isdiffpos"], filters = self._default_filters)

		# check that we have anything
		if len(tuptup) == 0:
			self.why = "nothing passed default filter".format(self.maxNbad)
			self.logger.info(self.why)
			return None

		# now get the tuples
		rb_arr, jd_arr, magnr_arr, isdiffpos_arr = map(np.array, zip(*tuptup))

		# if we have only one detection, something is likely to be wrong
		if len(jd_arr) == 1:
			self.why ="only detection passes default filter"
			self.logger.info(self.why)
			return None


		# check that source is not too bright in ZTF ref img
		if self.brightRefMag > np.min(magnr_arr) > 0:
			self.why = "min(magnr)={0:0.2f}, which is < {1:0.1f}".format(np.min(magnr_arr), self.brightRefMag)
			self.logger.info(self.why)
			return None

		# if we want, only check last observation 
		if self.LastOnly:
			
			lastcheck = alert.get_values(
				"jd", filters = self._default_filters + [
					{
						'attribute': 'jd', 
						'operator': '==', 
						'value': max(alert.get_values('jd'))
					}
				]
			)

			if len(lastcheck) == 0:
				self.why = "last detection did not pass default filter".format(self.maxNbad)
				self.logger.info(self.why)
				return None

			rb_arr = [rb_arr[np.argmax(jd_arr)]] # make sure rb check below is only for last detection


		# if no detections pass real bogus, remove
		if max(rb_arr) < self.minRealBogusScore:
			self.why = "max(rb)={0:0.2f}, which is  < {1:0.2f}".format(max(rb_arr), self.minRealBogusScore)
			self.logger.info(self.why)

		
		# do cut on moving sources (with all good detections)
		dt = abs(np.sort(jd_arr)[1:] - np.sort(jd_arr)[0:-1])

		# first check that we dont have bunch of duplicates
		if not sum(dt > 0):
			self.why = "number of detections={0}, but time difference between all is zero".format(len(dt))
			self.logger.info(self.why)
			return None
				
		dt = dt[dt > 0]
		if np.max(dt) < self.minDeltaJD:
			self.why = "potential mover, number of good detections={0}; max(time diff)={1:1.3f} h, which is < {2:0.3f} h".format(len(jd_arr), max(dt)*24, self.minDeltaJD*24)
			self.logger.info(self.why)
			return None 

		# Require two good detections withone one night 
		if np.min(dt) > self.maxDeltaJD:
			self.why = "number of good detections={0}; min(time diff)={1:1.1f} h, which is > {2:0.1f} h".format(len(jd_arr), min(dt)*24, self.maxDeltaJD*24)
			self.logger.info(self.why)
			return None 


		# if we make it this far, compute the host-flare distance, using only (decent-enough) detections
		distnr_arr, sigmapsf_arr, rb_arr, fwhm_arr, fid_arr = \
		 map(np.array, zip(*alert.get_ntuples(["distnr", "sigmapsf","rb","fwhm", "fid"], filters=self._default_filters)))

		
		# compute a few different measures of the distance
		# we also compute these for each band seperately
		rb_arr = np.clip(rb_arr, 0.01, 1) 				# remove zero scores 
		my_weight = 1/(np.clip(fwhm_arr, 1.5,10)*np.clip(sigmapsf_arr, 0.01, 1)) 	# combine differen measures for how good the distnr measurement is

		idx_all = np.repeat(True, len(distnr_arr))
		idx_g = fid_arr == 1
		idx_r = fid_arr == 2
		
		for idx, bnd in zip([idx_g, idx_r, idx_all],['g','r','r+g']):
			
			if sum(idx):
				
				mean_distnr = np.mean(distnr_arr[idx])
				weighted_distnr = np.sum(distnr_arr[idx]*my_weight[idx])/sum(my_weight[idx])
				median_distnr = np.median(distnr_arr[idx])
				
				if mean_distnr < self.maxDeltaRad:
					self.why = "pass on mean distnst={0:0.2f}, band={1}; detections used={2}".format(mean_distnr, bnd, sum(idx))
					self.logger.info(self.why)
					return self.on_match_t2_units

				
				if median_distnr < self.maxDeltaRad:
					self.why = "pass on median distnst={0:0.2f}, band={1}; detections ued={2}".format(median_distnr, bnd, sum(idx))
					self.logger.info(self.why)
					return self.on_match_t2_units 

				
				if weighted_distnr < self.maxDeltaRad:
					self.why = "pass on weighted distnst={0:0.2f}, band={1}; detections used={2}".format(weighted_distnr, bnd, sum(idx))
					self.logger.info(self.why)
					return self.on_match_t2_units

		# if none of the measures of the host-flare pass the cut, reject this alert
		self.why = "mean/median/weighted distnr = ({0:0.2f}/{1:0.2f}/{2:0.2f}), which is > {0:0.2f}".format(mean_distnr, median_distnr, weighted_distnr, self.maxDeltaRad)
		self.logger.info(self.why)
		return None 

		# we could also do some some more simple checks for mean color and delta magnitude here, 
		# but perhaps that's better as a T2 module



# Nowhere used
# 
#	def get_default_filters(self):
#		''''
#		returns a copy of default filter list that is applied when we run get_ntuples()
#		'''
#
#		# try to make a copy of this list that will remain untouched as the dictionaries are moved around
#		if self._default_filters is dict:
#			return self._default_filters.copy()
#		else:				
#			return [flt.copy() for flt in self._default_filters]
