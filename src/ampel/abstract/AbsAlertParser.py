#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertParser.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.04.2018
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from abc import ABC, abstractmethod

class AbsAlertParser(ABC):


	@abstractmethod
	def parse(self, byte_stream):
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
