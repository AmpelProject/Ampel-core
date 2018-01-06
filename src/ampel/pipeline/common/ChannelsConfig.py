#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/ChannelsConfig.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2018
# Last Modified Date: 06.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.ChannelFlags import ChannelFlags


class ChannelsConfig:

	def __init__(self, d_config_channels=None, config_file=None):
		"""
		"""
		if not config_file is None:
			self.config = self.load_config_from_file(config_file)
		else:
			self.config = d_config_channels

		self.d_chanlabel_chanflag = dict()
		self.d_chanlabel_t2sflag = dict()


	def load_config_from_file(self, file_name):
		"""
		"""
		import json
		with open(file_name, "r") as data_file:
			config = json.load(data_file)

		return config


	def get_available_channel_names(self):
		return self.config["channels"].keys()

	
	def get_channel_parameters(self, channel_name):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		return self.config[channel_name]['parameters']
		

	def get_channel_flag_name(self, channel_name):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		return self.config[channel_name]['flagName']


	def get_channel_flag_instance(self, channel_name):
		"""	
			Generate ChannelFlag for channel_name if not previously generated,
			otherwise return previously generated enum flag
		"""	
		
		# Check for unknown channel names
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		# Check for previously generated enum flag
		if channel_name in self.d_chanlabel_chanflag:
			return self.d_chanlabel_chanflag[channel_name] 

		# Create enum flag instance
		flag = ChannelFlags[self.config[channel_name]['flagName']]
		self.d_chanlabel_chanflag[channel_name] = flag

		return flag


	def get_channel_t2s_flag(self, channel_name):
		"""	
			Generate T2ModuleIds for channel_name if not previously generated,
			otherwise return previously generated enum flag
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		# Check for previously generated enum flag
		if channel_name in self.d_chanlabel_t2sflag:
			return self.d_chanlabel_t2sflag[channel_name] 

		t2s_flag = T2ModuleIds(0)

		for d_entry in self.config[channel_name]['t2Modules'].values():
			t2s_flag |= T2ModuleIds[d_entry['module']]

		self.d_chanlabel_t2sflag[channel_name] = t2s_flag

		return t2s_flag 


	def get_channel_filter_config(self, channel_name):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		return self.config[channel_name]['alertFilter']


	def get_channel_t2_param(self, channel_name, t2_module_name):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		for el in self.config[channel_name]['t2Modules']:
			if el['module'] == t2_module_name:
				return el['paramId']

		return None 
