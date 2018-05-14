#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/ZIAlertParser.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2018
# Last Modified Date: 04.05.2018
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


	def shape(self, in_dict):
		"""	
		Returns a dict. See AbsAlertParser docstring for more info
		The dictionary with index 0 in the pps list is the most recent photopoint.
		"""

		try:
	
			if in_dict['prv_candidates'] is None:

				return {
					'pps': [in_dict['candidate']],
					'ro_pps': ImmutableList(
						[ImmutableDict(in_dict['candidate'])]
					),
					'uls': None,
					'ro_uls': None,
					'tran_id': in_dict['objectId'],
					'alert_id': in_dict['candid']
				}

			else:
	
				uls_list = []
				ro_uls_list = []
				pps_list = [in_dict['candidate']]
				ro_pps_list = [ImmutableDict(in_dict['candidate'])]
	
				for el in in_dict['prv_candidates']:
	
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
	 								'programid': el['programid'], 
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
					'ro_uls': None if len(uls_list) == 0 else ImmutableList(ro_uls_list),
					'tran_id': in_dict['objectId'],
					'alert_id': in_dict['candid']
				}

		except:
			if in_dict is not None:
				self.logger.critical("Exception occured while loading alert", exc_info=1)
			return None
