#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/t3/TranContentConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 07.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator, ValidationError
from typing import Dict, Union, List, Any
from ampel.common.AmpelUtils import AmpelUtils
from ampel.common.docstringutils import gendocstring
from ampel.config.AmpelBaseModel import AmpelBaseModel
from ampel.config.t3.LoadableContent import LoadableContent
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils

@gendocstring
class QueryMatchConfig(AmpelBaseModel):
	""" 

	Note: If logic parameter is a string or a simple list, 
	it is interpreted as if it was 'anyOf' (OR operator)

	.. sourcecode:: python\n

		In []: QueryMatchConfig(**{"field":"ab", "logic":'3'})
		Out[]: <QueryMatchConfig field='ab' logic={'anyOf': ['3']}>
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":['3', '1', '2']})
		Out[]: <QueryMatchConfig field='ab' logic={'anyOf': ['3', '1', '2']}>
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'allOf': ['3', '1', '2']}})
		Out[]: <QueryMatchConfig field='ab' logic={'allOf': ['3', '1', '2']}>
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'anyOf': ['3', '1', '2']}})
		Out[]: <QueryMatchConfig field='ab' logic={'anyOf': ['3', '1', '2']}>
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', '1', '2']}})
		Out[]: <QueryMatchConfig field='ab' logic={'anyOf': [{'allOf': ['1', '2']}, '3', '1', '2']}>
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', {'allOf': ['1','3']}]}})
		Out[]: <QueryMatchConfig field='ab' logic={'anyOf': [{'allOf': ['1', '2']}, '3', {'allOf': ['1', '3']}]}>
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":['1', '2', ['1', '2', '3']]})
		Out[]: Unsupported nesting (err 1)
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'allOf': ['1', '2', ['1','2']]}})
		Out[]: Unsupported nesting (err 5)
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'allOf': ['1', '2'], 'abc': '2'}})
		Out[]: Unsupported dict format {'allOf': ['1', '2'], 'abc': '2'}
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'anyOf': [{'anyOf': ['1','2']}, '2']}})
		Out[]: Unsupported nesting (err 3)
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', {'anyOf': ['1','2']}]}})
		Out[]: Unsupported nesting (err 3)
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'allOf': [{'allOf': ['1','2']}, '3', '1', '2']}})
		Out[]: Unsupported nesting (err 5)
		
		In []: QueryMatchConfig(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', {'allOf': ['1',{'allOf':['1','2']}]}]}})
		Out[]: Unsupported nesting (err 4)
	"""


	field: str
	logic: Any


	@validator('logic', whole=True, pre=True)
	def check_format(cls, v, values, **kwargs):

		#print("--------------------------------")
		#print("QueryMatchConfig: v: %s" % str(v))
		#print("QueryMatchConfig: kwargs: %s" % kwargs)
		#print("QueryMatchConfig: values: %s" % values)

		if type(v) is str:
			return {'anyOf': [v]}

		if type(v) is list:
			if not AmpelUtils.check_seq_inner_type(v, str):
				cls.print_and_raise(
					header="QueryMatchConfig error",
					msg="Unsupported nesting (err 1)"
				)
			return {'anyOf': v}

		if type(v) is dict:

			if len(v) != 1:
				cls.print_and_raise(
					header="QueryMatchConfig error",
					msg="Unsupported dict format %s" % v
				)

			if 'anyOf' in v:

				if not AmpelUtils.is_sequence(v['anyOf']):
					cls.print_and_raise(
						header="QueryMatchConfig error",
						msg="Invalid dict value type: %s. Must be a sequence" % type(v['anyOf'])
					)

				# 'anyOf' supports only a list of dicts and str
				if not AmpelUtils.check_seq_inner_type(v['anyOf'], (str, dict), multi_type=True):
					cls.print_and_raise(
						header="QueryMatchConfig error",
						msg="Unsupported nesting (err 2)"
					)

				for el in v['anyOf']:

					if isinstance(el, dict):

						if 'anyOf' in el:
							cls.print_and_raise(
								header="QueryMatchConfig error",
								msg="Unsupported nesting (err 3)"
							)

						elif 'allOf' in el:

							# 'allOf' closes nesting  
							if not AmpelUtils.check_seq_inner_type(el['allOf'], str):
								cls.print_and_raise(
									header="QueryMatchConfig error",
									msg="Unsupported nesting (err 4)"
								)
						else:
							cls.print_and_raise(
								header="QueryMatchConfig error",
								msg="Unsupported dict: %s" % el
							)

			elif 'allOf' in v:

				if not AmpelUtils.is_sequence(v['allOf']):
					cls.print_and_raise(
						header="QueryMatchConfig error",
						msg="Invalid dict value type: %s. Must be a sequence" % type(v['anyOf']))

				# 'allOf' closes nesting  
				if not AmpelUtils.check_seq_inner_type(v['allOf'], str):
					cls.print_and_raise(
						header="QueryMatchConfig error",
						msg="Unsupported nesting (err 5)"
					)
			else: 
				cls.print_and_raise(
					header="QueryMatchConfig error",
					msg="Invalid dict key (only 'anyOf' and 'allOf' allowed)"
				)

		return v
