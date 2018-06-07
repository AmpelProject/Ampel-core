#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.05.2018
# Last Modified Date: 04.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class DBUtils():

	number_map = {
		'10': 'a', '11': 'b', '12': 'c', '13': 'd', '14': 'e', '15': 'f',
		'16': 'g', '17': 'h', '18': 'i', '19': 'j', '20': 'k', '21': 'l',
		'22': 'm', '23': 'n', '24': 'o', '25': 'p', '26': 'q', '27': 'r',
		'28': 's', '29': 't', '30': 'u', '31': 'v', '32': 'w', '33': 'x',
		'34': 'y', '35': 'z'
	}

	letter_map = {
		 'a': '10', 'b': '11', 'c': '12', 'd': '13', 'e': '14', 'f': '15',
		 'g': '16', 'h': '17', 'i': '18', 'j': '19', 'k': '20', 'l': '21',
		 'm': '22', 'n': '23', 'o': '24', 'p': '25', 'q': '26', 'r': '27',
		 's': '28', 't': '29', 'u': '30', 'v': '31', 'w': '32', 'x': '33',
		 'y': '34', 'z': '35'
	}

	@staticmethod
	def get_ampel_name(ztf_name):
		"""	
		Returns an int. 
		"""
		letter_map = DBUtils.letter_map
		return int(
			"".join(
				(	
					ztf_name[3:5], 
					letter_map[ztf_name[5]], 
					letter_map[ztf_name[6]], 
					letter_map[ztf_name[7]], 
					letter_map[ztf_name[8]], 
					letter_map[ztf_name[9]], 
					letter_map[ztf_name[10]], 
					letter_map[ztf_name[11]]
				)
			)
		)


	@staticmethod
	def get_ztf_name(db_long):
		"""	
		Returns a string. 
		"""
		str_long = str(db_long)
		number_map = DBUtils.number_map

		return "ZTF%s%s%s%s%s%s%s%s" % (
			str_long[0:2],
			number_map[str_long[2:4]],
			number_map[str_long[4:6]],
			number_map[str_long[6:8]],
			number_map[str_long[8:10]],
			number_map[str_long[10:12]],
			number_map[str_long[12:14]],
			number_map[str_long[14:16]]
		)
