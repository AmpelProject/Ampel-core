#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : src/ampel/contrib/ztfbh/t0/NuclearFilter.py
# Author            : sjoertvv <sjoert@umd.edu>
# Date              : 02.26.2018
# Last Modified Date: 
# Last Modified By  : svv

import numpy as np
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter
#from ampel.flags.T2RunnableIds import T2RunnableIds 


class TFilter(AbstractTransientFilter):
	"""
		Your filter must inherit the abstract parent class 'AbstractTransientFilter'
		The following three methods *must* be implemented:
			-> get_version(self)
			-> set_filter_parameters(self, d)
			-> apply(self, ampel_alert)
		The instance variable self.logger (inherited from the parent class) is ready-to-use.
	"""

	# Static version info
	version = 0.1

	def __init__(self):
		"""
		Constructor (optional)
		
		sjoertvv: removed the logger for now to make the __init__.py more easy

		"""
		self.logger.info("Logger for NuclearFilter")
		self.logger.debug("The log entries emitted by this logger will be stored into the Ampel DB")
		self.logger.debug("This logger is to be used 'as is', please don't change anything :)")

	
	def get_version(self):
		"""
		Mandatory implementation.
		"""
		return TFilter.version


	def get_default_filters(self):
		''''
		returns a copy of default filter list that is applied when we run get_ntuples()
		'''

		# try to make a copy of this list that will remain untouched as the dictionaries are moved around
		if self._default_filters is dict:
			return self._default_filters.copy()
		else:
			filter_list_out = []
		for flt in self._default_filters:
			filter_list_out.append(flt.copy())
		return filter_list_out


	def set_filter_parameters(self, d):
		"""
		Mandatory implementation.
		This method is called automatically before alert processing.
		Parameter 'd' is a dict instance loaded from the ampel config. 
		"""
		
		self.MaxDeltaRad 		= d['MaxDeltaRad']		# the max flare-ref distance (distnr) to be accepted, we try mulitple ways of summing distnr for multiple detection 
		self.MaxDeltaRadLate 	= d['MaxDeltaRadLate'] 	# not used yet
		self.MinSgscore 		= d['MinSgscore']		# star-galaxy scaore in PS1 (this seems to be either zero or one)
		self.MaxNbad 			= d['MaxNbad']			# max bad pixels, applied before mover cuts
		self.MinDeltaJD			= d['MinDeltaJD'] 		# remove movers with min time distance in days between detections with nbad<=MaxNbad		
		self.MinRealBogusScore 	= d['MinRealBogusScore']# min RealBogus score of *any* observation
		self.BrightPS1RMag 		= d['BrightPS1RMag']	# bright star removal: min PS1 r-band mag 
		self.BrightRefMag 		= d['BrightRefMag'] 	# bright star removal: used for both ZTF filters
		self.LastOnly 			= d['LastOnly'] 		# use only most recent detection (attempt to simulate real time)

		#self.on_match_default_flags = T2RunnableIds.PHOTO_Z # need to update this with own T2? 
		self.on_match_default_flags = True # need to update this with own T2? 

		# Instance here a dictionary later used in the method apply 		
		nbad_flt = {'nbad':self.MaxNbad, "operator": '<='}	# not too many bad pixels
		isdiff_flt1 = {'isdiffpos':0, "operator":'!='}	 	# is not ref-science
		isdiff_flt1 = {'isdiffpos':'0', "operator":'!='}	 	# is not ref-science
		isdiff_flt2 = {'isdiffpos':'f', "operator":'!='}		# is not ref-science (again!)
		distnr_flt = {'distnr':0, "operator": '>'}			# has host galaxy detected (removes orphans)

		self._default_filters = [nbad_flt, distnr_flt, isdiff_flt1, isdiff_flt2]

	def apply(self, alert):
		"""
		Mandatory implementation.
		To exclude the alert, return *None*
		To accept it, either 
			* return self.on_match_default_flags
			* return a custom combination of T2RunnableIds

		Make a selection on:
		- the distance between the transient and host in reference image
		- the Real/Bogus sore
		- the distance to a bright star 
		"""		

		# first check we have an extended source (note this can remove flares from faint galaxies that missclassified in PS1)
		# these will have to be dealt with in the orphan/faint filter		
		sgscore = alert.get_values("sgscore")
		if len(sgscore)==0:
			sgscore = alert.get_values("sgscore1")[0]
		else:
			sgscore = sgscore[0]
			
		if sgscore is None:		
			self.why="sgscore=None"
			self.logger.info(self.why)
			return None

		if sgscore>self.MinSgscore:				
				self.why="sgscore={0:0.1f}, which is > {1:0.1f}".format(sgscore, self.MinSgscore)
				self.logger.info(self.why)
				return None

		srmag = alert.get_values("srmag")
		if len(srmag)==0:
			srmag = alert.get_values("srmag1")
			if len(srmag)==0:
				srmag=None
			else:
				srmag = srmag[0]	
		else:
			srmag = srmag[0]

		if srmag is None:
			self.why = "sr mag is None"
			self.logger.info(self.why)
			return None
		
		if self.BrightPS1RMag > srmag > 0:
			self.why = "PS1 r-band mag={0:0.2f}, which is < {1:0.2f}".format(srmag, self.BrightPS1RMag)
			self.logger.info(self.why)
			return None

		these_filters =self.get_default_filters()

		# get RealBogus scores for observations, check number of bad pixels
		tuptup = alert.get_ntuples(["rb","jd", "magnr", "isdiffpos"], filters=these_filters)		

		# check that we have anything
		if len(tuptup)==0:
			self.why = "nothing passed default filter, nbad<={0} & isdiffpos==1 & distnr>0".format(self.MaxNbad)
			self.logger.info(self.why)
			return None

		# now get the tuples
		rb_arr, jd_arr, magnr_arr, isdiffpos_arr = map(np.array, zip(*tuptup))


		# if we want, only check last detection 
		if self.LastOnly:
			
			these_filters.append( {'jd':max(alert.get_values("jd")),'operator':'='} )			
			lastcheck = alert.get_values("jd", filters=these_filters)

			if len(lastcheck)==0:
				self.why = "last detection did not pass default filter, nbad<={0} & isdiffpos==1 & distnr>0".format(self.MaxNbad)
				self.logger.info(self.why)
				return None		 	
			rb_arr = [rb_arr[np.argmax(jd_arr)]] # make sure rb check below is only for last detection


		# if no detections pass real bogus, remove
		if max(rb_arr)<self.MinRealBogusScore:
			self.why = "max(rb)={0:0.2f}, which is  < {1:0.2f}".format(max(rb_arr), self.MinRealBogusScore)
			self.logger.info(self.why)
			return None

		# do cut on moving sources
		dt = np.max(jd_arr) - jd_arr
		if np.max(dt)<self.MinDeltaJD:
			self.why = "potential mover, number of detections={0}; max(time diff)={1:1.3f} h, which is <{2:0.3f} h".format(len(dt), max(dt)*24, self.MinDeltaJD*24)
			self.logger.info(self.why)
			return None 

		# check that source is not too bright in ZTF ref img
		if self.BrightRefMag > np.min(magnr_arr) > 0:
			self.why = "min(magnr)={0:0.2f}, which is < {1:0.1f}".format(np.min(magnr_arr), self.BrightRefMag)
			self.logger.info(self.why)
			return None

		# if we make it this far, compute the host-flare distance, using only (decent-enough) detections
		distnr_arr, sigmapsf_arr, rb_arr, fwhm_arr, fid_arr = \
		 map(np.array, zip(*alert.get_ntuples(["distnr", "sigmapsf","rb","fwhm", "fid"], filters=these_filters)))

		
		# compute a few different measures of the distance
		# we also compute these for each band seperately

		rb_arr = np.clip(rb_arr, 0.01, 1) 				# remove zero scores 
		my_weight = 1/rb_arr*fwhm_arr*sigmapsf_arr 		# combine differen measures for how good the distnr measurement is
		my_weight = 1/np.clip(my_weight, 0.001,1000) 		# protection against goblins

		idx_all = np.repeat(True, len(distnr_arr))
		idx_g = fid_arr == 1
		idx_r = fid_arr == 2
		
		for idx, bnd in zip([idx_all, idx_g, idx_r],['r+g', 'g','r']):
			
			if sum(idx):
				
				mean_distnr = np.mean(distnr_arr[idx])
				weighted_distnr = np.sum(distnr_arr[idx]*my_weight[idx])/sum(my_weight[idx])
				median_distnr = np.median(distnr_arr[idx])
				
				if mean_distnr<self.MaxDeltaRad:
					self.why = "pass on mean distnst={0:0.2f}, band={1}; detections used={2}".format(mean_distnr, bnd, sum(idx))
					self.logger.info(self.why)
					return self.on_match_default_flags

				
				if median_distnr<self.MaxDeltaRad:
					self.why = "pass on median distnst={0:0.2f}, band={1}; detections ued={2}".format(median_distnr, bnd, sum(idx))
					self.logger.info(self.why)
					return self.on_match_default_flags 

				
				if weighted_distnr<self.MaxDeltaRad:
					self.why = "pass on weighted distnst={0:0.2f}, band={1}; detections used={2}".format(weighted_distnr, bnd, sum(idx))
					self.logger.info(self.why)
					return self.on_match_default_flags

		# if none of the measures of the host-flare pass the cut, reject this alert
		self.why = "distnr > {0:0.2f}".format(self.MaxDeltaRad)
		self.logger.info(self.why)
		return None 


		# we could also do some some more simple checks for mean color and delta magnitude here, 
		# but perhaps that's better as a T2 module


