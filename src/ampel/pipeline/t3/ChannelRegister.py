#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/ChannelRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.03.2018
# Last Modified Date: 16.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class ChannelRegister:
	"""
	Class used to save the association between given channel names  
	and instances of the ampel classes:
	- ampel.base.LightCurve 
	- ampel.base.ScienceRecord

	Used by TransientLoader when forks of transient instances are likely to happen
	"""

	def __init__(self, tran_id):
		"""
		"""
		self.tran_id = tran_id
		self.science_records = {}
		self.lightcurves = {}
		self.latest_state = {}


	def add_science_record(self, channels, science_record):
		"""
		Saves association between given science_record instance and provided channels
		channels: list of strings whereby each element is a string channel id 
				  (example: ['HU_EARLY_SN', 'HU_RANDOM'])
		science_record: instance of ampel.base.ScienceRecord
		"""
		for channel in channels:

			if channel not in self.science_records:
				self.science_records[channel] = [science_record]
			else:
				self.science_records[channel].append(science_record)


	def add_lightcurve(self, channels, lightcurve):
		"""
		Saves association between given lightcurve instance and provided channels
		channels: list of strings whereby each element is a string channel id 
				  (example: ['HU_EARLY_SN', 'HU_RANDOM'])
		lightcurve: instance of ampel.base.LightCurve
		"""
		for channel in channels:

			if channel not in self.lightcurves:
				self.lightcurves[channel] = [lightcurve]
			else:
				self.lightcurves[channel].append(lightcurve)


	def add_latest_state(self, channel, state):
		"""
		Saves latest state of transient for the provided channel
		"""
		self.latest_state[channel] = state


	
	def get_latest_state(self, channel, state):
		"""
		Retrieves the saved latest state of transient for the provided channel
		channel: string id of the channel (example: 'HU_EARLY_SN')
		state: sting id of the state (example: '9fc0ffe8c17208453daf6ae0c0b720a6')
		"""
		if channel not in self.latest_state:
			return None

		return self.latest_state[channel]

	
	def get_lightcurves(self, channel):
		"""
		Retrieves the saved lightcurve instances associated with provided channel
		channel: string id of the channel (example: 'HU_EARLY_SN')
		"""
		if channel not in self.lightcurves:
			return None

		return self.lightcurves[channel]


	def get_science_records(self, channel):
		"""
		Retrieves the saved science record instances associated with provided channel
		channel: string id of the channel (example: 'HU_EARLY_SN')
		"""
		if channel not in self.science_records:
			return None

		return self.science_records[channel]
