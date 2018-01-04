#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/utils/T2DocsBuilder.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 04.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, hashlib
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.ChannelFlags import ChannelFlags


class T2DocsBuilder():
	"""
	"""

	def __init__(self, channels_config, channel_names_to_load):
		"""
		Creates the variable 'dd_full_t2Ids_paramIds_chanlist'

        To insert t2 docs in a way that is not prone to race conditions (and optimizes T2 computations)
        we make use of the dict structure dd_full_t2Ids_paramIds_chanlist:
 	
          - SNCOSMO 
            - default
                CHANNEL_SN|CHANNEL_LENS
            - mySetting
                CHANNEL_GRB
          - PHOTO_Z 
          - default
              CHANNEL_SN|CHANNEL_LENS|CHANNEL_GRB
			
		Each root entry in the dd_full_t2Ids_paramIds_chanlist dict is also a dict 
		(with the different possible paramIds as key)
			
		1st dict key: t2 module id (integer)
		2nd key: t2 module paramId (string)
		
		Values are combinations of channel flags (ampel.flags.ChannelFlags)
		"""

		self.channel_names_to_load = channel_names_to_load
		self.dd_full_t2Ids_paramIds_chanlist = dict()

		self.active_chan_flags = [
			channels_config.get_channel_flag(chan_name) 
			for chan_name in channel_names_to_load
		]

		# Loop through schedulable t2 modules 
		for t2_module_id in T2ModuleIds:

			# Each entry of dd_full_t2Ids_paramIds_chanlist is also a dict (1st dict key: t2 module id)
			self.dd_full_t2Ids_paramIds_chanlist[t2_module_id] = dict()

			# Loop through the t0 channels 
			for i, chan_name in enumerate(channel_names_to_load):

				# Extract default paramId (= parameter ID = wished configuration) associated with 
				# with current T2 module and for the current T0 channel only.
				# t2_module_id is of type 'enum', we access the flag label with the attribute 'name'
				paramId = channels_config.get_t2_module_param(chan_name, t2_module_id.name)

				# if paramId was not found, it means the current t2_module_id 
				# was not registered for the present channel
				if paramId is None:
					continue
				
				# if paramId key was not yet stored in dd_full_t2Ids_paramIds_chanlist struct
				# create an empty ChannelFlags enum flag
				if not paramId in self.dd_full_t2Ids_paramIds_chanlist[t2_module_id]:
					self.dd_full_t2Ids_paramIds_chanlist[t2_module_id][paramId] = ChannelFlags(0)

				# Add current t0 channel to dd_full_t2Ids_paramIds_chanlist
				# For example: dd_full_t2Ids_paramIds_chanlist[SCM_LC_FIT]["default"] |= CHANNEL_SN
				self.dd_full_t2Ids_paramIds_chanlist[t2_module_id][paramId] |= self.active_chan_flags[i] 

		
	def reduce_unsorted(self, compound_gen, dict_of_scheduled_t2_modules):
		"""
			dict_of_scheduled_t2_modules example:
			d[CHANNEL_SN] = SNCOSMO | AGN
				whereby CHANNEL_SN is of type ChannelFlag
				and SNCOSMO | AGN is of type T2ModuleIds
		"""
		array_of_scheduled_t2_modules = []
		
		for chan_flag in self.active_chan_flags:
			for key in dict_of_scheduled_t2_modules.keys():
				if key == chan_flag:
					array_of_scheduled_t2_modules.append(dict_of_scheduled_t2_modules[key])	

		self.reduce(compound_gen, array_of_scheduled_t2_modules)
	

	def reduce(self, compound_gen, array_of_scheduled_t2_modules):
		"""
		----------------------------------------------------------------------------------
		The following task is bit complex, hence the lengthy explanation.
		----------------------------------------------------------------------------------
		TLDR: This function computes and returns a dict structure t2s_eff required 
		for creating T2 docs. This computation is necessary since:
			* T0 filters can return a customized the list of T2 modules to be run 
			* The photopoint compoundId of a given alert can be different between channels (since those 
			  can have different config parameters [include autoComplete points, custom exclusions, etc])
		----------------------------------------------------------------------------------
		
		The input parameter of alert ingesters is a list ('array_of_scheduled_t2_modules') 
		of combinations of scheduled T2 module Ids (ampel.flags.T2ModulesIds set by T0 filters). 
		The size of array_of_scheduled_t2_modules corresponds to the number of "active" channel 
		('active' meaning that the T0 AlertProcessor has loaded the channel)
		
		Strictly considered, an example of array_of_scheduled_t2_modules could be:
		  [<T2ModuleIds.PHOTO_Z|AGN: 12>, <T2ModuleIds.PHOTO_Z|SNCOSMO: 9>, etc...]
		
		where each position in the array corresponds to a specific channel flag (ampel.flags.ChannelFlags)
		(info stored in self.active_chan_flags):
		  [CHANNEL_GRB, CHANNEL_SN, etc...]

		In a more compact form for the sake of this documentation, 
		let's say array_of_scheduled_t2_modules provides the following information:
		
		  CHANNEL_SN: SNCOSMO | PHOTO_Z
		  CHANNEL_GRB: GRB_FIT | PHOTO_Z | SNCOSMO 
		
		One important fact to keep in mind is that T0 filters can 'customize' scheduled T2s, 
		which means we could have alternatively:
		
		  CHANNEL_SN: SNCOSMO 
		  CHANNEL_GRB: GRB_FIT | PHOTO_Z 
		
		----------------------------------------------------------------------------------
		
		in __init__, this class has set - based on the ampel db config doc - the value of
		self.dd_full_t2Ids_paramIds_chanlist which as an illustration could look like this:
		
		  - SNCOSMO 
		    - default
		        CHANNEL_SN | CHANNEL_LENS
		    - mySetting
		        CHANNEL_GRB
		  - PHOTO_Z 
		    - default
		        CHANNEL_SN | CHANNEL_LENS | CHANNEL_GRB
		  - GRB_FIT:
		    - default
		        CHANNEL_GRB
		
		Note: self.dd_full_t2Ids_paramIds_chanlist is built only using 'active' channels.
		
		----------------------------------------------------------------------------------
		
		INNER LOOP 1 matches the function parameter 'array_of_scheduled_t2_modules' with 
		'dd_full_t2Ids_paramIds_chanlist' in order to create 't2s_eff' (eff=effective) 
		which - based on the example with 'customized' scheduled T2s - would look like this:
		
		  - SNCOSMO 
		    - default
		        CHANNEL_SN
		  - PHOTO_Z 
		    - default
		        CHANNEL_SN|CHANNEL_GRB
		  - GRB_FIT:
		    - default
		        CHANNEL_GRB

		----------------------------------------------------------------------------------

		INNER LOOP 2 adds a third dimension to t2s_eff in order to take the fact into account
		that an alert loaded by different channel might result in different compounds

		Let us consider the previous example, and only PHOTO_Z with param "default"
		INNER LOOP 2 makes sure that CHANNEL_SN & CHANNEL_GRB are associated with the same compound 
		(by checking compoundId equality), otherwise different t2 *docs* should be created:
			
	    If compound ids differ between 	                  If compound ids are equals,
		CHANNEL_SN & CHANNEL_GRB, *two*                   *one* t2 doc will be created
		t2 docs will be created                 

		 - PHOTO_Z                                        - PHOTO_Z
		 	- default                                        - default
			  - compoundid: a1b2c3                             - compoundid: a1b2c3
					CHANNEL_SN               VS                  CHANNEL_SN|CHANNEL_GRB
			  - compoundid: d4c3b2a1                       
					CHANNEL_GRB
		"""

		t2s_eff = {}

		##################
		## INNER LOOP 1 ##
		##################

		# loop through all channels, 
		# get scheduled T2s (single_channel_scheduled_t2s) for each channel (self.active_chan_flags[i])
		for i, single_channel_scheduled_t2s in enumerate(array_of_scheduled_t2_modules):

			# Skip Nones (current channel with index i has rejected this transient)
			if single_channel_scheduled_t2s is None:
				continue

			# loop through scheduled module ids
			for t2_module_id in single_channel_scheduled_t2s.as_list():

				# Create dict instance if necessary	
				if not t2_module_id in t2s_eff:
					t2s_eff[t2_module_id] = {}

				# loop through all known paramIds for this t2 module
				for paramId in self.dd_full_t2Ids_paramIds_chanlist[t2_module_id].keys():
				
					# If channel flag of current channel (index i) is registered in dd_full_t2Ids_paramIds_chanlist
					if self.active_chan_flags[i] in self.dd_full_t2Ids_paramIds_chanlist[t2_module_id][paramId]:

						# create flag if necessary
						if not paramId in t2s_eff[t2_module_id]:
							t2s_eff[t2_module_id][paramId] = ChannelFlags(0)

						# append t0 channel value
						t2s_eff[t2_module_id][paramId] |= self.active_chan_flags[i]


		##################
		## INNER LOOP 2 ##
		##################

		# Loop through t2 module ids
		for t2_id in t2s_eff.keys():

			# Loop through parameter ids for current module 
			for param_id in t2s_eff[t2_id].keys():

				# Copy ChannelFlags	that was set in INNER LOOP 1
				chan_flags = t2s_eff[t2_id][param_id]

				# Add a 3rd dimmension to t2s_eff dict
				t2s_eff[t2_id][param_id] = dict()

				# Set channel values for each compound id. 
				for compound_id in compound_gen.get_compound_ids(chan_flags):
					t2s_eff[t2_id][param_id][compound_id] = compound_gen.get_channels_for_compoundid(compound_id)
				

		return t2s_eff

		# Output example:
		# - PHOTO_Z
		#	- default
		#	  - compoundid: a1b2c3
		#			CHANNEL_SN | CHANNEL_LEN | CHANNEL_5
		#	  - compoundid: PHOTO_Z                       
		#			CHANNEL_GRB 
		#
		# Or represented differently 
		# t2s_eff['PHOTO_Z']['default']['a1b2c3'] = CHANNEL_SN | CHANNEL_LEN | CHANNEL_5
		# t2s_eff['PHOTO_Z']['default']['PHOTO_Z'] = CHANNEL_GRB


