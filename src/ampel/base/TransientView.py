#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/TransientView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 28.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime
from bson.binary import Binary
from ampel.base.Frozen import Frozen
from ampel.flags.TransientFlags import TransientFlags
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class TransientView(Frozen):
	"""
	Container class referencing:
	-> possibly various instances of objects:
		* ampel.base.PlainPhotoPoint
		* ampel.base.PlainUpperLimit
		* ampel.base.Compound
		* ampel.base.LightCurve
		* ampel.base.T2Record
	Instances of this class are provided to T3 modules and are typically 
	generated using a TransientData instance created by DBContentLoader.
	"""

	def __init__(self, 
		tran_id, flags, created, modified, journal, latest_state=None,
		photopoints=None, upperlimits=None, compounds=None, 
		lightcurves=None, t2records=None, channel=None, logger=None
	):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.tran_id = tran_id
		self.flags = flags
		self.created = created
		self.modified = modified
		self.journal = journal
		self.latest_state = latest_state
		self.photopoints = photopoints
		self.upperlimits = upperlimits
		self.compounds = compounds
		self.lightcurves = lightcurves
		self.t2records = t2records
		self.channel = channel
		self.__isfrozen = True


	def print_info(self):
		""" """
		TransientView._print_info(self, self.logger)


	def get_flags(self):
		""" """
		return self.flags


	def get_latest_lightcurve(self):
		""" 
		"""
		if self.latest_state is None:
			self.logger.warn('Request for latest lightcurve cannot complete (latest state not set)')
			return None

		if len(self.lightcurves) == 0:
			self.logger.warn('Request for latest lightcurve cannot complete (No lightcurve was loaded)')
			return None

		res = next(filter(lambda x: x.id == self.latest_state, self.lightcurves), None)
		if res is None:
			self.logger.warn(
				'Request for latest lightcurve cannot complete (Lightcurve %s not found)' % 
				self.latest_state		
			)
			return None

		return res


	def get_latest_state(self, to_hex=False):
		""" """
		if self.latest_state is None:
			self.logger.warn('Request for latest state cannot complete (not set)')
			return None

		return self.latest_state.hex() if to_hex else self.latest_state


	def get_photopoints(self):
		"""
		Returns a dict instance with 
			-> key: photopoint id
			-> value: instance of ampel.base.PlainPhotoPoint
		Returns None if PhotoPoints were not loaded
		"""
		return self.photopoints
	

	def get_upperlimits(self):
		"""
		Returns a dict instance with 
			-> key: photopoint id
			-> value: instance of ampel.base.PlainUpperLimit
		Returns None if UpperLimits were not loaded
		"""
		return self.upperlimits


	def get_compounds(self, copy=False):
		"""
		Returns a tuple of MappingProxyType instances or None if Compounds were not loaded 
		"""
		return self.compounds


	def get_compound(self, compound_id):
		""" 
		Returns an instance of MappingProxyType or None if no Compound exists with the provided id
		lightcurve_id: either a bson Binary instance (with subtype 5) or a string with length 32
		"""
		if type(compound_id) is str:
			compound_id = Binary(bytes.fromhex(compound_id), 5)
		return next(filter(lambda x: x['_id'] == compound_id, self.compounds), None)


	def get_lightcurves(self):
		"""
		Returns a tuple of ampel.base.LightCurve instances or None if Compounds were not loaded 
		"""
		return self.lightcurves

	
	def get_lightcurve(self, lightcurve_id):
		""" 
		Returns an instance of ampel.base.LightCurve 
		or None if no LightCurve exists with the provided lightcurve_id
		lightcurve_id: either a bson Binary instance (with subtype 5) or a string with length 32
		"""
		if type(lightcurve_id) is str:
			lightcurve_id = Binary(bytes.fromhex(lightcurve_id), 5)
		return next(filter(lambda x: x.id == lightcurve_id, self.lightcurves), None)


	def get_science_records(self, t2_unit_id=None, latest=False):
		""" 
		Returns an instance or a tuple of instances of ampel.base.ScienceRecord 
		t2_unit_id: string. Limit returned science record(s) to the one with the provided t2 unit id
		latest: boolean. Whether to return the latest science record(s) or not
		"""
		if latest:

			if self.latest_state is None:
				return None
		
			if t2_unit_id is None:
				return next(
					filter(
						lambda x: x.compound_id == self.latest_state, 
						self.t2records
					), None
				)
			else:
				return next(
					filter(
						lambda x: x.compound_id == self.latest_state and x.t2_unit_id == t2_unit_id, 
						self.t2records
					), None
				)
		else:

			if t2_unit_id is None:
				return self.t2records
			else:
				return tuple(
					filter(
						lambda x: x.compound_id == self.latest_state, 
						self.t2records
					)
				)


	def get_journal_entries(self, tier=None, t3JobName=None, latest=False):
		""" 
		"""
		if tier is None and t3JobName is None:
			entries = self.journal
		else:
			if None in (tier, t3JobName):
				entries = (
					tuple(filter(lambda x: x.get('t3JobName') == t3JobName, self.journal)) if tier is None
					else tuple(filter(lambda x: x.get('tier') == tier, self.journal))
				)
			else:
				entries = tuple(filter(
					lambda x: x.get('tier') == tier and x.get('t3JobName') == t3JobName, self.journal
				))

		if len(entries) == 0:
			return None

		return entries[-1] if latest else entries


	def get_time_created(self, format_time=None):
		""" """
		if self.journal is None or len(self.journal) == 0:
			return None
		return self._get_time(self.journal[0], format_time)


	def get_time_modified(self, format_time=None):
		""" """
		if self.journal is None or len(self.journal) == 0:
			return None
		return self._get_time(self.journal[-1], format_time)

		
	def _get_time(self, entry, format_time=None):
		""" """
		if format_time is None:
			return entry['dt']
		else:
			return datetime.fromtimestamp(entry['dt']).strftime(
				'%d/%m/%Y %H:%M:%S' if format_time is True else format_time
			)


	@staticmethod
	def _print_info(tran, logger):
		""" 
		"""
		logger.info("#"*30)

		logger.info(" -> Ampel ID: %i" % 
			(tran.tran_id)
		)

		# pylint: disable=no-member
		if TransientFlags.INST_ZTF in tran.flags:
			logger.info(" -> ZTF ID: %s" % 
				(AmpelUtils.get_ztf_name(tran.tran_id))
			)

		if tran.channel is not None:
			logger.info(" -> Channel: %s" % str(tran.channel))

		created = tran.get_time_created(True)
		modified = tran.get_time_modified(True)
		logger.info(" -> Created: %s" % created if created is not None else 'Not available')
		logger.info(" -> Modified: %s" % modified if modified is not None else 'Not available')
		logger.info(" -> Flags: %s" % (tran.flags if tran.flags is not None else "not set"))
		logger.info(" -> Latest state: %s" % 
			(tran.latest_state.hex() if tran.latest_state is not None else "not set")
		)
		logger.info(" -> Transient elements: PP: %i, UL: %i, CP: %i, LC: %i, SR: %i" % 
			(
				len(tran.photopoints) if tran.photopoints is not None else 0, 
				len(tran.upperlimits) if tran.upperlimits is not None else 0, 
				len(tran.compounds) if tran.compounds is not None else 0, 
				len(tran.lightcurves) if tran.lightcurves is not None else 0, 
				len(tran.t2records) if tran.t2records is not None else 0
			)
		)
		logger.info("#"*30)
