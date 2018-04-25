#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/ZIAlertParser.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertParser import AbsAlertParser
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from werkzeug.datastructures import ImmutableDict, ImmutableList
import fastavro


class ZIAlertParser(AbsAlertParser):
	"""
	ZI: shortcut for ZTF IPAC.

	This class is responsible for:
		* Parsing IPAC generated ZTF Alerts (using fastavro)
		* Splitting the list of photopoints contained in 'prv_candidates' in two lists:
			* one pps list
			* one upper limit list
		* Casting the photopoint & upper limits dicts into ImmutableDicts
		* Extracting the unique transient and alert id
		* Returning a dict containing these values
	"""

	def __init__(self, logger=None):
		self.logger = LoggingUtils.get_logger() if logger is None else logger


	def parse(self, byte_stream):
		"""	
		Loads an avro alert with fastavro using the provided byte stream.
		Returns a dict. See AbsAlertParser docstring for more info
		The dictionary with index 0 in the pps list is the most recent photopoint.
		"""

		try:

			reader = fastavro.reader(byte_stream)
			avro_dict = next(reader, None)
	
			if avro_dict['prv_candidates'] is None:
				return {
					'pps': [avro_dict['candidate']],
					'ro_pps': ImmutableList(
						[ImmutableDict(avro_dict['candidate'])]
					),
					'uls': None,
					'ro_uls': None,
					'tran_id': avro_dict['objectId'],
					'alert_id': avro_dict['candid']
				}
			else:
	
				uls_list = []
				ro_uls_list = []
				pps_list = [avro_dict['candidate']]
				ro_pps_list = [ImmutableDict(avro_dict['candidate'])]
	
				for el in avro_dict['prv_candidates']:
	
					if el['candid'] is None:
						uls_list.append(el)
						ro_uls_list.append(
							# Twice better %timeit performance than ImmutableDict(el)
							ImmutableDict(
								{ 
									'jd': el['jd'], 
									'fid': el['fid'], 
									'pid': el['pid'], 
	 								'diffmaglim': el['diffmaglim'], 
									'pdiffimfilename': el['pdiffimfilename'] 
								}
							)
						)
					else:
						pps_list.append(el)
						ro_pps_list.append(ImmutableDict(el))
	
				return {
					'pps': pps_list,
					'ro_pps': ImmutableList(ro_pps_list),
					'uls': None if len(uls_list) == 0 else uls_list,
					'ro_uls': None if len(uls_list) == 0 else ImmutableList(uls_list),
					'tran_id': avro_dict['objectId'],
					'alert_id': avro_dict['candid']
				}

		except:
			self.logger.exception("Exception occured while loading alert")
			return None




	@staticmethod
	def load_raw_dict_from_file(file_path):
	# TODO: move convenience method somewhere else
		"""	
		Load avro alert using fastavro. 
		A dictionary instance (or None) is returned 
		"""	
		with open(file_path, "rb") as fo:
			reader = fastavro.reader(fo)
			zavro_dict = next(reader, None)

		return zavro_dict

