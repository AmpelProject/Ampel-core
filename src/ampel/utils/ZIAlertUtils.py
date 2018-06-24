#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/utils/ZIAlertUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.06.2018
# Last Modified Date: 25.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, fastavro, tarfile, os
from datetime import datetime
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.base.LightCurve import LightCurve
from ampel.base.TransientView import TransientView
from ampel.base.PlainPhotoPoint import PlainPhotoPoint
from ampel.base.PlainUpperLimit import PlainUpperLimit

class ZIAlertUtils:

	# pylint: disable=no-member
	photo_flags = PhotoFlags.INST_ZTF|PhotoFlags.SRC_IPAC
	# pylint: disable=no-member
	tran_flags = TransientFlags.INST_ZTF|TransientFlags.SRC_IPAC

	@staticmethod
	def to_lightcurve(file_path=None, content=None):
		"""
		Creates and returns an instance of ampel.base.LightCurve using a ZTF IPAC alert.
		"""
		return ZIAlertUtils._create_lc(
			*ZIAlertUtils._shape(
				ZIAlertUtils._get_alert_content(file_path, content)
			)
		)


	@staticmethod
	def to_transientview(file_path=None, content=None):
		"""
		Creates and returns an instance of ampel.base.LightCurve using a ZTF IPAC alert.
		"""
		alert_content = ZIAlertUtils._get_alert_content(file_path, content)
		now = datetime.utcnow().timestamp()
		lc = ZIAlertUtils._create_lc(
			*ZIAlertUtils._shape(alert_content), now
		)

		return TransientView(
			alert_content['objectId'], ZIAlertUtils.tran_flags, now, now, 
			[{'dt': now, 'tier': 0, 'loadedBy': 'ZIAlertUtils'}],
			lightcurves=[lc], upperlimits=lc.ulo_list, photopoints=lc.ppo_list
		)


	@staticmethod
	def _create_lc(pps, uls, now=None):
		""" """
		return LightCurve(
			os.urandom(16), 
			[PlainPhotoPoint(el, ZIAlertUtils.photo_flags, read_only=True) for el in pps], 
			[PlainUpperLimit(el, ZIAlertUtils.photo_flags, read_only=True) for el in uls] if uls else None, 
			info={'tier': 0, 'added': datetime.utcnow().timestamp() if now is None else now}, 
			read_only=True
		)


	@staticmethod
	def _get_alert_content(file_path=None, content=None):
		""" """
		# deserialize extracted alert content
		if file_path is not None:
			with open(file_path, 'rb') as f:
				content = ZIAlertUtils._deserialize(f)

		if content is None:
			raise ValueError("Illegal parameter")

		return content


	@staticmethod
	def _deserialize(f):
		""" """
		reader = fastavro.reader(f)
		return next(reader, None)


	@staticmethod
	def _shape(alert_content):
		""" """
		if alert_content.get('prv_candidates') is not None:
			pps = [el for el in alert_content['prv_candidates'] if el.get('candid') is not None]
			pps.insert(0,  alert_content['candidate'])
			return pps, [el for el in alert_content['prv_candidates'] if el.get('candid') is None]
		else:
			return [alert_content['candidate']], None
