#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/ingest/T2DocsBluePrint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Sequence
from ampel.model.t0.APChanModel import APChanModel


class T2DocsBluePrint:
	"""
	Creates a nested dict struct that is used as basis to create T2 documents.
	The generated structure (create_blueprint) is optimized: 
	-> A T2 documents for a given compound shared among different channels is referenced only once.
	"""

	def __init__(self, t2_units: List[str], t2_units_using_uls: List[str], chan_bundle: Sequence[APChanModel]):
		"""
		Parameters:
		:param t2_units: all schedulable t2s for the given t0 processes
		:param chan_bundle: list of instances of ampel.t0.Channel
		NOTE: the order of the APChanConfig instance listed in chan_bundle matters: 
		the parameter 'array_of_scheduled_t2_units' used in method 'create_blueprint' 
		must have the same channel order 
		:param t2_units_using_uls: list/set of t2 unit names making use of upper limits

		Purpose:
		Creates the variable 'dd_full_t2Ids_runConfigs_chanlist'

        To insert t2 docs in a way that is not prone to race conditions (and optimizes T2 computations)
        we make use of the dict structure dd_full_t2Ids_runConfigs_chanlist:
 	
          - SNCOSMO 
            - default
                {"CHANNEL_SN", "CHANNEL_LENS"}
            - myCustomRunConfig
                {"CHANNEL_GRB"}
          - PHOTO_Z 
          - default
              {"CHANNEL_SN", "CHANNEL_LENS", "CHANNEL_GRB"}
			
		Each root entry in the dd_full_t2Ids_runConfigs_chanlist dict is also a dict 
		(with the different possible paramIds as key)
			
		1st dict key: t2 runnable id (integer)
		2nd key: t2 runnable run_config (string)
		
		Values are a set of channel names
		"""

		if not isinstance(chan_bundle, Sequence):
			raise ValueError("Parameter bundle must be a sequence of APChanModel instances")

		if len(chan_bundle) == 0:
			raise ValueError("Parameter bundle cannot be empty")

		self.dd_full_t2Ids_runConfigs_chanlist = {}
		self.chan_bundle = chan_bundle

		# T2 unit making use of upper limits
		self.t2_units_using_uls = t2_units_using_uls

		# Loop through schedulable t2 units
		for t2_id in t2_units:

			# Each entry of dd_full_t2Ids_runConfigs_chanlist is also a dict (1st dict key: t2 runnable id)
			self.dd_full_t2Ids_runConfigs_chanlist[t2_id] = {}

			# Loop through the t0 channel configurations
			for ap_chan_data in chan_bundle:
			
				# Extract default run_config (= run parameter ID = wished configuration) associated with 
				# with current T2 unit and for the current T0 channel only.
				t2_execution_unit = next(
					filter(
						lambda x: x.class_name == t2_id, 
						ap_chan_data.t0_add.t2_compute
					), None
				)

				# if t2_execution_unit was not found, it means the current t2_id 
				# was not registered for the present channel
				if t2_execution_unit is None:
					continue

				# shortcut
				run_config = t2_execution_unit.run_config
				
				# if run_config key was not yet stored in dd_full_t2Ids_runConfigs_chanlist struct
				# create an empty ChannelFlags enum flag
				if not run_config in self.dd_full_t2Ids_runConfigs_chanlist[t2_id]:
					self.dd_full_t2Ids_runConfigs_chanlist[t2_id][run_config] = {ap_chan_data.name}
				else:
					# Add current t0 channel to dd_full_t2Ids_runConfigs_chanlist
					# For example: dd_full_t2Ids_runConfigs_chanlist[SCM_LC_FIT]["default"].add("CHANNEL_SN")
					self.dd_full_t2Ids_runConfigs_chanlist[t2_id][run_config].add(ap_chan_data.name)

		
	def create_blueprint(self, compound_blueprint, array_of_scheduled_t2_units):
		"""
		----------------------------------------------------------------------------------
		The following task is bit complex, hence the lengthy explanation.
		----------------------------------------------------------------------------------
		TLDR: This function computes and returns a dict structure t2s_eff required 
		for creating T2 docs. This computation is necessary since:
			* T0 filters can return a customized the list of T2 runnables to be run 
			* The photopoint compoundId of a given alert can be different between channels (since those 
			  can have different config parameters [include autoComplete points, custom exclusions, etc])
		----------------------------------------------------------------------------------
		
		The input parameter of alert ingesters is a list ('array_of_scheduled_t2_units') 
		of combinations of scheduled T2 unit Ids ({set of T2 unit names} returned by T0 filters). 
		The size of array_of_scheduled_t2_units corresponds to the number of "active" channel 
		('active' meaning that the T0 AlertProcessor has loaded the channel)
		
		A concrete example of array_of_scheduled_t2_units could be:
		  [{"PHOTO_Z", "AGN"}, {"PHOTO_Z", "SNCOSMO"}, etc...]
		
		where each position in the array corresponds to a specific channel name
		  ["CHANNEL_GRB", "CHANNEL_SN", etc...]

		let's bring the two info above in a more compact form for the sake of this documentation:
		Say array_of_scheduled_t2_units provides the following information:
		
		  CHANNEL_SN: {"SNCOSMO", "PHOTO_Z"}
		  CHANNEL_GRB: {"GRB_FIT", "PHOTO_Z", "SNCOSMO"} 
		
		One important fact to keep in mind is that T0 filters can 'customize' scheduled T2s, 
		which means we could have alternatively:
		
		  CHANNEL_SN: {"SNCOSMO"} 
		  CHANNEL_GRB: {"GRB_FIT", "PHOTO_Z"} 
		
		----------------------------------------------------------------------------------
		
		in __init__ this class has set - based on info from the ampel db config - 
		the value of self.dd_full_t2Ids_runConfigs_chanlist which could look like this:
		
		  - SNCOSMO 
		    - default
		        {"CHANNEL_SN", "CHANNEL_LENS"}
		    - mySetting
		        CHANNEL_GRB
		  - PHOTO_Z 
		    - default
		        {"CHANNEL_SN", "CHANNEL_LENS", "CHANNEL_GRB"}
		  - GRB_FIT:
		    - default
		        {"CHANNEL_GRB"}
		
		Note: self.dd_full_t2Ids_runConfigs_chanlist is built only using 'active' channels.
		
		----------------------------------------------------------------------------------
		
		INNER LOOP 1 matches the function parameter 'array_of_scheduled_t2_units' with 
		'dd_full_t2Ids_runConfigs_chanlist' in order to create 't2s_eff' (eff=effective) 
		which - based on the example with 'customized' scheduled T2s - would look like this:
		
		  - SNCOSMO 
		    - default
		        {"CHANNEL_SN"}
		  - PHOTO_Z 
		    - default
		        {"CHANNEL_SN", "CHANNEL_GRB"}
		  - GRB_FIT:
		    - default
		        {"CHANNEL_GRB"}

		----------------------------------------------------------------------------------

		INNER LOOP 2 adds a third level to the dict t2s_eff in order to take into account 
		that an alert loaded by different channels might result in different compounds!

		Let's consider the previous example but limited to PHOTO_Z with param "default" only.
		INNER LOOP 2 makes sure that CHANNEL_SN & CHANNEL_GRB are associated with the same compound 
		(by checking compoundId equality), otherwise different t2 *docs* should be created:
			
	    If compound ids differ between 	                  If compound ids are equals,
		CHANNEL_SN & CHANNEL_GRB, *two*                   *one* t2 doc will be created
		t2 docs will be created                 

		 - PHOTO_Z                                        - PHOTO_Z
		 	- default                                        - default
			  - compoundid: a1b2c3d4                           - compoundid: a1b2c3d4
					{"CHANNEL_SN"}           VS                    {"CHANNEL_SN", "CHANNEL_GRB"}
			  - compoundid: d4c3b2a1                       
					{"CHANNEL_GRB"}
		"""

		t2s_eff = {}

		##################
		## INNER LOOP 1 ##
		##################

		# loop through all channels, 
		# get scheduled T2s (single_channel_scheduled_t2s) for each channel (self.active_chan_flags[i])
		for i, single_channel_scheduled_t2s in enumerate(array_of_scheduled_t2_units):

			# Skip Nones (current channel with index i has rejected this transient)
			if single_channel_scheduled_t2s is None:
				continue

			ap_chan_data = self.chan_bundle[i]

			# loop through scheduled runnable ids
			for t2_id in single_channel_scheduled_t2s:

				# Create dict instance if necessary	
				if not t2_id in t2s_eff:
					t2s_eff[t2_id] = {}

				# loop through all known paramIds for this t2 runnable
				for run_config in self.dd_full_t2Ids_runConfigs_chanlist[t2_id].keys():
				
					# If channel flag of current channel (index i) is registered in dd_full_t2Ids_runConfigs_chanlist
					if ap_chan_data.name in self.dd_full_t2Ids_runConfigs_chanlist[t2_id][run_config]:

						# create flag if necessary
						if not run_config in t2s_eff[t2_id]:
							t2s_eff[t2_id][run_config] = {ap_chan_data.name}
						else:
							# append t0 channel value
							t2s_eff[t2_id][run_config].add(ap_chan_data.name)


		##################
		## INNER LOOP 2 ##
		##################

		# Loop through t2 runnable ids
		for t2_id in t2s_eff.keys():

			# Loop through run configs for current t2 runnable 
			for run_config in t2s_eff[t2_id].keys():

				# Copy ChannelFlags	that was set in INNER LOOP 1
				chan_names = t2s_eff[t2_id][run_config]

				# Add a 3rd level to t2s_eff dict
				t2s_eff[t2_id][run_config] = dict()

				# Set channel values for each compound id. 
				# TODO: add t2_id as argument ! 
				# (t2 with 'use_upper_limits' == True will have different t2s)
				# compound_blueprint.get_compound_ids(chan_names, t2_unit)
				compound_ids = (
					compound_blueprint.get_effids_of_chans(chan_names) if t2_id in self.t2_units_using_uls
					else compound_blueprint.get_ppids_of_chans(chan_names)
				)

				# Simple case: there is only one compound id for the current association of channels
				if len(compound_ids) == 1:
					t2s_eff[t2_id][run_config][next(iter(compound_ids))] = chan_names
				else:
					# channels have different compound id.
					# we must compute the intersection (&) between chan_names from loop 1 and 
					# the set of channels returned by CompoundGenerator for a given compound_id
					for compound_id in compound_ids:
						t2s_eff[t2_id][run_config][compound_id] = (
							(
								compound_blueprint.get_chans_with_effid(compound_id) if t2_id in self.t2_units_using_uls
								else compound_blueprint.get_ppids_of_chans(compound_id)
							)
							& chan_names
						)
				
		return t2s_eff

		# Output example:
		# - PHOTO_Z
		#	- default
		#	  - compoundid: a1b2c3d4
		#			CHANNEL_SN | CHANNEL_LEN | CHANNEL_5
		#	  - compoundid: d4c3b2a1                       
		#			CHANNEL_GRB 
		#
		# Or using a different representation:
		# t2s_eff['PHOTO_Z']['default']['a1b2c3d4'] = CHANNEL_SN | CHANNEL_LEN | CHANNEL_5
		# t2s_eff['PHOTO_Z']['default']['d4c3b2a1'] = CHANNEL_GRB
