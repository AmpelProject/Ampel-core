#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/Transient.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 03.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.DBUtils import DBUtils
from ampel.flags.TransientFlags import TransientFlags
from ampel.base.Frozen import Frozen
import logging


class Transient(Frozen):
	"""
	Container class referencing:
	-> possibly various instances of objects:
		* ampel.base.PlainPhotoPoint
		* ampel.base.PlainUpperLimit
		* ampel.base.Compound
		* ampel.base.LightCurve
		* ampel.base.T2Record
	Instances of this class are typically generated 
	from a TransientView by TransientLoader 
	and provided to T3 modules.
	"""

	def __init__(self, 
		tran_id, flags, created, modified, latest_state=None,
		photopoints=None, upperlimits=None, compounds=None, 
		lightcurves=None, t2records=None, channel=None, logger=None
	):
		"""
		Parameters:
		* tran_id: transient id (string)
		* logger: logger instance from python module 'logging'

		Convert internal lists Photopoint / Upperlimits / Lightcurves and Science Records 
		to tuple and freeze this class. PLease note that the dicts referenced by the provided
		lists are not changed. It means that these objects (Photopoints...) must be instantiated 
		with read_only=True before they are added to this class (methods add_photopoints, ....)
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.tran_id = tran_id
		self.flags = flags
		self.created = created
		self.modified = modified
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
		Transient._print_info(self, self.logger)


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


	def get_lightcurves(self):
		""" """
		return self.lightcurves

	
	def get_lightcurve(self, lightcurve_id):
		""" """
		return self.lightcurves[lightcurve_id]


	def get_photopoints(self, copy=False):
		"""
		Returns a dict instance
		-> key: photopoint id
		-> value: instance of ampel.base.PlainPhotoPoint
		-> dict can be empty if PhotoPoints were not loaded 
		   (see load_options of class TransientLoader)
		"""
		return self.photopoints if copy is False else self.photopoints.copy()
	

	def get_upperlimits(self, copy=False):
		"""
		Returns a dict instance
		-> key: upperlimit id
		-> value: instance of ampel.base.PlainUpperLimit
		-> dict can be empty if UpperLimits were not loaded 
		   (see load_options of class TransientLoader)
		"""
		return self.upperlimits if copy is False else self.upperlimits.copy()


	def get_compound(self, compound_id):
		""" 
		argument 'compound_id' must be a python integer 
		"""
		if compound_id not in self.compounds:
			return None

		return self.compounds[compound_id]


	def get_compounds(self, copy=False):
		"""
		Returns a dict instance
		-> key: compound id
		-> value: instance of ampel.base.Compound
		-> dict can be empty if Compounds were not loaded 
		   (see load_options of class TransientLoader)
		"""
		return self.compounds if copy is False else self.compounds.copy()


	def get_science_records(self, t2_unit_id=None, latest=False):
		""" 
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


	@staticmethod
	def _print_info(tran, logger):
		""" 
		"""

		logger.info("#"*30)

		logger.info(" -> Ampel ID: %i" % 
			(tran.tran_id)
		)

		if TransientFlags.INST_ZTF in tran.flags:
			logger.info(" -> ZTF ID: %s" % 
				(DBUtils.get_ztf_name(tran.tran_id))
			)

		if tran.channel is not None:
			logger.info(" -> Channel: %s" % tran.channel)

		logger.info(" -> Created: %s" % 
			(tran.created.strftime('%d/%m/%Y %H:%M:%S') if tran.created is not None else "not set")
		)

		logger.info(" -> Modified: %s" % 
			(tran.modified.strftime('%d/%m/%Y %H:%M:%S') if tran.modified is not None else "not set")
		)

		logger.info(" -> Flags: %s" % 
			(tran.flags if tran.flags is not None else "not set")
		)

		logger.info(
			" -> Latest state: %s" % 
			(tran.latest_state.hex() if tran.latest_state is not None else "not set")
		)

		logger.info(
			" -> Transient elements: PP: %i, UL: %i, CP: %i, LC: %i, SR: %i" % 
			(
				len(tran.photopoints), len(tran.upperlimits), len(tran.compounds), 
				len(tran.lightcurves), len(tran.t2records)
			)
		)
		logger.info("#"*30)
