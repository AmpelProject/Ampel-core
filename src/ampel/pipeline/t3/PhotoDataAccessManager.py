#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/PhotoDataAccessManager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.06.2018
# Last Modified Date: 22.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.base.flags.PhotoFlags import PhotoFlags
from ampel.pipeline.config.channel.ChannelConfig import ChannelConfig

class PhotoDataAccessManager:
	"""
	BETA CLASS.
	TODO: make it instrument specific and move it to Ampel-ZTF, Ampel-ASASN, etc...
	"""

	def __init__(self, chan_name):
		"""
		"""

		self.photo_flags = None
		chan = AmpelConfig.get_config(['channels', chan_name])

		if chan is None:
			raise ValueError(
				"Unknown channel name: %s (type: %s), please check your config" % 
				(chan_name, type(chan_name))
			)

		channel = ChannelConfig.create(tier=0, **chan)

		###############
		# TODO: improve
		###############

		stream_config = channel.get_stream_config("ZTFIPAC")

		# pylint: disable=no-member
		self.photo_flags = PhotoFlags.INST_ZTF

		if not stream_config.parameters['ZTFPartner']:
			self.photo_flags |= PhotoFlags.ZTF_PUBLIC


		# Loop through channel sources (ZTFIPAC, ASASN, etc..)
#		for src_name in channel.get_sources().keys():
#
#			# pylint: disable=unsubscriptable-object
#			current_photo_flag = PhotoFlags[
#				AmpelConfig.get_config('global.sources.%s.flags.photo' % src_name)			
#			]
#
#			if (
#				src_name == "ZTFIPAC" and 
#				not channel.get_config("parameters.ZTFPartner", source="ZTFIPAC")
#			):
#				current_photo_flag |= PhotoFlags.ZTF_PUBLIC
#
#			if current_photo_flag is not None:
#				if self.photo_flags is None:
#					self.photo_flags = current_photo_flag
#				else:
#					if type(self.photo_flags) is list:
#						self.photo_flags.append(current_photo_flag)
#					else:
#						self.photo_flags = [self.photo_flags, current_photo_flag]
					

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
		argument 'photopoint' must be an instance of ampel.base.(Plain)PhotoPoint 
		"""

		return tuple(
			pp for pp in photopoints.values()
			if self.check_flags(pp, self.photo_flags)
		)


	def get_upperlimits(self, upperlimit):
		""" 
		argument 'upperlimit' must be an instance of ampel.base.(Plain)UpperLimit 
		"""

		return tuple(
			pp for pp in upperlimit.values()
			if self.check_flags(pp, self.photo_flags)
		)

