#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/utils/DBWired.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.09.2018
# Last Modified Date: 04.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class DBWired:
	""" 
	"""

	config_col_names = {
		'all':	(
			'global', 'channels', 't0_filters', 't2_units', 't2_run_config', 
			't3_jobs', 't3_run_config', 't3_units', 'resources'
		),
		0: (
			'global', 'channels', 't0_filters', 
			't2_units', 't2_run_config'
		),
		2: (
			'global', 't2_units', 't2_run_config'
		)
	}


	@staticmethod
	def get_config_from_db(db, tier='all'):
		""" """
		config = {}

		for col_name in DBWired.config_col_names[tier]:

			colName = DBWired.to_camel_case(col_name)
			config[colName] = {}

			for el in db[col_name].find({}):
				config[colName][el.pop('_id')] = el

		return config


	@staticmethod
	def to_camel_case(snake_str):
		""" """
		components = snake_str.split('_')
		return components[0] + ''.join(x.title() for x in components[1:])
