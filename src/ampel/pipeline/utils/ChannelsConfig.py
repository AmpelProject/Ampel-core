#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/utils/ChannelsConfig.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.01.2018
# Last Modified Date: 27.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.T2RunnableIds import T2RunnableIds
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

	
	def get_channel_input_parameters(self, channel_name, instrument="ZTF", alerts="IPAC"):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		for el in self.config[channel_name]['input']:
			if el['instrument'] == instrument and el['alerts'] == alerts:
				d = el

		return d['parameters']
		

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
		flag = ChannelFlags[channel_name]
		self.d_chanlabel_chanflag[channel_name] = flag

		return flag


	def get_channel_t2s_flag(self, channel_name):
		"""	
			Generate T2RunnableIds flags for channel_name if not previously generated,
			otherwise return previously generated enum flag
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		# Check for previously generated enum flag
		if channel_name in self.d_chanlabel_t2sflag:
			return self.d_chanlabel_t2sflag[channel_name] 

		t2s_flag = T2RunnableIds(0)

		for d_entry in self.config[channel_name]['t2Compute']:
			t2s_flag |= T2RunnableIds[d_entry['id']]

		self.d_chanlabel_t2sflag[channel_name] = t2s_flag

		return t2s_flag 


	def get_channel_filter_config(self, channel_name):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		return self.config[channel_name]['t0Filter']


	def set_channel_filter_parameter(self, channel_name, param_name, param_value):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		self.config[channel_name]['alertFilter']['parameters'][param_name] = param_value


	def get_channel_t2_param(self, channel_name, t2_runnable_name):
		"""	
			Dict path lookup shortcut function
		"""	
		if channel_name not in self.config:
			raise ValueError('Channel %s not found' % channel_name)

		for el in self.config[channel_name]['t2Compute']:
			if el['id'] == t2_runnable_name:
				return el['paramId']

		return None 
