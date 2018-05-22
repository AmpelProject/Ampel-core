#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/ZIAlertShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2018
# Last Modified Date: 21.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertShaper import AbsAlertShaper
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from types import MappingProxyType


class ZIAlertShaper(AbsAlertShaper):
	"""
	ZI: shortcut for ZTF IPAC.
	This class is responsible for:
	* Parsing IPAC generated ZTF Alerts
	* Splitting the list of photopoints contained in 'prv_candidates' in two lists:
		* a pps list
		* a upper limit list
	* Casting the photopoint & upper limits dicts into MappingProxyType
	* Extracting the unique transient and alert id
	* Returning a dict containing these values
	"""

	letter_map = {
		 'a': '10', 'b': '11', 'c': '12', 'd': '13', 'e': '14', 'f': '15',
		 'g': '16', 'h': '17', 'i': '18', 'j': '19', 'k': '20', 'l': '21',
		 'm': '22', 'n': '23', 'o': '24', 'p': '25', 'q': '26', 'r': '27',
		 's': '28', 't': '29', 'u': '30', 'v': '31', 'w': '32', 'x': '33',
		 'y': '34', 'z': '35'
	}

	def __init__(self, logger=None):
		"""	"""	
		self.logger = LoggingUtils.get_logger() if logger is None else logger


	def shape(self, in_dict):
		"""	
		Returns a dict. See AbsAlertParser docstring for more info
		The dictionary with index 0 in the pps list is the most recent photopoint.
		"""

		try:

			ztf_id = in_dict['objectId']
			letter_map = ZIAlertShaper.letter_map
			int_tran_id = int(
				"".join(
					(	
						ztf_id[3:5], 
						letter_map[ztf_id[5]], 
						letter_map[ztf_id[6]], 
						letter_map[ztf_id[7]], 
						letter_map[ztf_id[8]], 
						letter_map[ztf_id[9]], 
						letter_map[ztf_id[10]], 
						letter_map[ztf_id[11]]
					)
				)
			)
	
			if in_dict['prv_candidates'] is None:

				return {
					'pps': [in_dict['candidate']],
					'ro_pps': (MappingProxyType(in_dict['candidate']),) ,
					'uls': None,
					'ro_uls': None,
					'tran_id': int_tran_id,
					'ztf_id': ztf_id,
					'alert_id': in_dict['candid']
				}

			else:
	
				uls_list = []
				ro_uls_list = []
				pps_list = [in_dict['candidate']]
				ro_pps_list = [MappingProxyType(in_dict['candidate'])]
	
				for el in in_dict['prv_candidates']:
	
					if el['candid'] is None:
						uls_list.append(el)
						ro_uls_list.append(
							MappingProxyType(
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
						ro_pps_list.append(MappingProxyType(el))
	
				return {
					'pps': pps_list,
					'ro_pps': tuple(el for el in ro_pps_list),
					'uls': None if len(uls_list) == 0 else uls_list,
					'ro_uls': None if len(uls_list) == 0 else tuple(el for el in ro_uls_list),
					'tran_id': int_tran_id,
					'ztf_id': ztf_id,
					'alert_id': in_dict['candid']
				}

		except:
			if in_dict is not None:
				self.logger.critical("Exception occured while loading alert", exc_info=1)
			return None
