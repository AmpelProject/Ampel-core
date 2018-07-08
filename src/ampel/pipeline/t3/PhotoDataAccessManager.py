#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/DataAccessManager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.06.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.base.flags.PhotoFlags import PhotoFlags
from ampel.pipeline.config.Channel import Channel
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class DataAccessManager:
	"""
	"""

	def __init__(self, chan_name):
		"""
		"""

		al_config = AmpelConfig.get_config()

		if chan_name not in al_config['channels']:
			raise ValueError("Unknown channel name '%s', please check your config" % chan_name)

		channel = Channel(chan_name, al_config['channels'][chan_name])
		self.photo_flags = None

		# Loop through channel sources (ZTFIPAC, ASASN, etc..)
		for src_name in channel.get_sources().keys():

			src_params = al_config['global']['sources'][src_name]

			current_photo_flag = None

			if 'flags' in src_params and 'photo' in src_params['flags']:
				# pylint: disable=unsubscriptable-object
				current_photo_flag = PhotoFlags[src_params['flags']['photo']]

			if (
				src_name == "ZTFIPAC" and 
				not channel.get_config("parameters.ZTFPartner", source="ZTFIPAC")
			):
				current_photo_flag |= PhotoFlags.ZTF_PUBLIC

			if current_photo_flag is not None:
				if self.photo_flags is None:
					self.photo_flags = current_photo_flag
				else:
					if type(self.photo_flags) is list:
						self.photo_flags.append(current_photo_flag)
					else:
						self.photo_flags = [self.photo_flags, current_photo_flag]
					

	def check_flags(self, el, flags):

		if flags is None:
			return False

		if type(flags) is list:
			if len(flags) == 1:
				return el.has_flags(flags[0])
			return any(el.has_flags(flags))

		return el.has_flags(flags)


	def get_photopoints(self, photopoints):
		""" 
		argument 'photopoint' must be an instance of ampel.base.core.PhotoPoint 
		"""

		return tuple(
			pp for pp in photopoints.values()
			if self.check_flags(pp, self.photo_flags)
		)


	def get_upperlimits(self, upperlimit):
		""" 
		argument 'upperlimit' must be an instance of ampel.base.core.UpperLimit 
		"""

		return tuple(
			pp for pp in upperlimit.values()
			if self.check_flags(pp, self.photo_flags)
		)

