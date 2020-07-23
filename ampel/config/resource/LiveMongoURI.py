#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/resource/LiveMongoURI.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : Unspecified
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.config.resource.resources import ResourceURI

class LiveMongoURI(ResourceURI):
	"""Connection to live transient database"""

	name = "mongo"
	fields = ('hostname', 'port')
	roles = ('writer', 'logger')

	@classmethod
	def get_default(cls):
		return dict(scheme='mongodb', hostname='localhost', port=27017)
