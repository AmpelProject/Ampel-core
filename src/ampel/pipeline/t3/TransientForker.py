#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientForker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.03.2018
# Last Modified Date: 04.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.Transient import Transient
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.AlDocTypes import AlDocTypes


class TransientForker:
	"""
	Create a new Transient instance based on the content (photopoints, lightcurves, science records)
	of a previously instanciated transient object using the provided selection criteria
	"""

	@staticmethod
	def fork(parent_tran, doc_type=None, state=None, channel=None, t2_unit_ids=None, pps_must_flags=None):
		"""
		Creates a new Transient instance based on the content (photopoints, lightcurves, science records)
		of a previously instanciated transient object using the provided selection criteria

		Parameters:
		doc_type: enum flag instance of ampel.flags.AlDocTypes
		state: md5 string of transient state (compound id)
		channel: string. Id of channel
		t2_unit_ids: list of t2 unit ids (list of strings)
		chan_input_params: list of input parameters. Default for now [<instance of ZIInputParameter>] 
		"""

		if channel is not None:
			if not hasattr(parent_tran, "channel_register"):
				raise ValueError(
					"Fork requireds parent transient object to have a ChannelRegister instance"
				)
			else:
				channel_register = parent_tran.channel_register

		# Instanciante new transient
		transient = Transient(parent_tran.tran_id, parent_tran.logger)
		transient.set_parameters(
			[
				("created", parent_tran.created),
				("modified", parent_tran.modified),
				("flags", parent_tran.flags)
			]
		)

		# Document type based trimming: lightcurves
		excl_lightcurves = (
			True if doc_type is not None and AlDocTypes.COMPOUND not in doc_type 
			else None
		)

		# Document type based trimming: photopoints
		excl_photopoints = (
			True if doc_type is not None and AlDocTypes.PHOTOPOINT not in doc_type 
			else None
		)

		# Document type based trimming: science records
		excl_science_records = (
			True if doc_type is not None and AlDocTypes.T2RECORD not in doc_type 
			else None
		)


		# Trim Lightcurves
		##################

		if parent_tran.lightcurves and not excl_lightcurves:

			if channel is not None:

				if state is not None:

					# The transient state (compound id) is saved in each 
					# ligthcurve instance within the instance variable 'id' 
					transient.lightcurves = {
						# channel_register.get_lightcurves() returns a list of
						# ampel.base.Lightcurves instances
						lc.id: lc for lc in channel_register.get_lightcurves(channel) 
						if lc.id == state
					}

				else:

					transient.lightcurves = {
						lc.id: lc for lc in channel_register.get_lightcurves(channel) 
					}
			else:

				# Fork of lightcurves was requested but no filter applies
				# -> all the lightcurves from the parent will be found in the fork intance
				transient.lightcurves = parent_tran.lightcurves.copy()


		# Trim PhotoPoints
		##################

		# Side note 1: base PhotoPoints instances come without policy. 
		# Side note 2: PhotoPoints instance can contain channel specific exclusions.
		# This information can 'leak', in the sense that everyone 
		# having access to a particular PhotoPoint can 'see' which channel
		# manually excluded a PhotoPoint. -> No issue imho
		if parent_tran.photopoints and not excl_photopoints:

			if channel is not None:

				# Make sure the fork for channel WHATEVER_PUBLIC cannot
				# contain PhotoPoint instances accessible only for ZTF partners.
				if pps_must_flags is not None:

					transient.photopoints = {
						key: val for key, val in parent_tran.photopoints.items()
						if not pps_must_flags in val.flags
					}

				else:

					# Reference everything as no other filter criteria apply for now
					transient.photopoints = parent_tran.photopoints.copy()
			else:

				# Reference everything
				transient.photopoints = parent_tran.photopoints.copy()


		# Trim science records
		######################

		if parent_tran.science_records and not excl_science_records:

			if channel is not None:

				if t2_unit_ids is not None:

					# Filter t2 records based on channel and provided t2 unit ids
					# (the 'state' filter may apply additionaly below)
					transient.science_records = {
						sr.t2_unit_id: sr for sr in channel_register.get_science_records(channel) 
						if sr.t2_unit_id in t2_unit_ids
					}

				else:

					# Filter t2 records based on channel only
					# (the 'state' filter may apply additionaly below)
					transient.science_records = {
						sr.t2_unit_id: sr for sr in channel_register.science_records(channel) 
					}
			else:	

				if t2_unit_ids is not None:

					# Filter t2 records based on provided t2 unit ids
					# (the 'state' filter may apply additionaly below)
					transient.science_records = {
						key: val for key, val in parent_tran.science_records.items()
						if key in t2_unit_ids
					}

				else:

					# Reference all t2 records
					# (the 'state' filter may apply additionaly below)
					transient.science_records = parent_tran.science_records.copy()

				# Apply state filter: remove t2 records associated with other 
				# transient states than the one provided.
				if state is not None:

					# Get lists of t2 records. Example of a such list:
					# {
					#	'SNCOSMO': [
					#		<ampel.base.ScienceRecord.ScienceRecord at 0x108d47b38>,
  					# 		<ampel.base.ScienceRecord.ScienceRecord at 0x108d47b00>,
  					#		<ampel.base.ScienceRecord.ScienceRecord at 0x108d47a90>
					#	]
					# }
					for science_records_list in transient.science_records.values():

						# Loop through the reversed list as we may delete list elements
						for science_records in reversed(science_records_list):

							# Check if the current science record in the list 
							# is associated with the right transient state
							if science_records.compound_id != state:

								# If not, remove it from the list
								science_records_list.remove(science_records)
								
		return transient
