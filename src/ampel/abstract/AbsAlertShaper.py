#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.04.2018
# Last Modified Date: 15.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from abc import ABC, abstractmethod

class AbsAlertShaper(ABC):


	@abstractmethod
	def shape(self, dict_instance):
		"""
		Should return a dict containing the following keywords:
		'pps': list of dicts
		'uls': list of dicts
		'ro_pps': list of werkzeug.ImmutableDict
		'ro_uls': list of werkzeug.ImmutableDict
		'tran_id': string
		'alert_id': long
		"""
		pass
