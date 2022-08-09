#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/t3/QueryMatchModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.09.2018
# Last Modified Date:  16.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.types import strict_iterable
from ampel.util.collections import check_seq_inner_type
from ampel.util.docstringutils import gendocstring
from ampel.base.AmpelBaseModel import AmpelBaseModel

@gendocstring
class QueryMatchModel(AmpelBaseModel):
	"""

	Note: If logic parameter is a string or a simple list,
	it is interpreted as if it was 'anyOf' (OR operator)

	.. sourcecode:: python\n

		In []: QueryMatchModel(**{"field":"ab", "logic":'3'})
		Out[]: <QueryMatchModel field='ab' logic={'anyOf': ['3']}>

		In []: QueryMatchModel(**{"field":"ab", "logic":['3', '1', '2']})
		Out[]: <QueryMatchModel field='ab' logic={'anyOf': ['3', '1', '2']}>

		In []: QueryMatchModel(**{"field":"ab", "logic":{'allOf': ['3', '1', '2']}})
		Out[]: <QueryMatchModel field='ab' logic={'allOf': ['3', '1', '2']}>

		In []: QueryMatchModel(**{"field":"ab", "logic":{'anyOf': ['3', '1', '2']}})
		Out[]: <QueryMatchModel field='ab' logic={'anyOf': ['3', '1', '2']}>

		In []: QueryMatchModel(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', '1', '2']}})
		Out[]: <QueryMatchModel field='ab' logic={'anyOf': [{'allOf': ['1', '2']}, '3', '1', '2']}>

		In []: QueryMatchModel(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', {'allOf': ['1','3']}]}})
		Out[]: <QueryMatchModel field='ab' logic={'anyOf': [{'allOf': ['1', '2']}, '3', {'allOf': ['1', '3']}]}>

		In []: QueryMatchModel(**{"field":"ab", "logic":['1', '2', ['1', '2', '3']]})
		Out[]: Unsupported nesting (err 1)

		In []: QueryMatchModel(**{"field":"ab", "logic":{'allOf': ['1', '2', ['1','2']]}})
		Out[]: Unsupported nesting (err 5)

		In []: QueryMatchModel(**{"field":"ab", "logic":{'allOf': ['1', '2'], 'abc': '2'}})
		Out[]: Unsupported dict format {'allOf': ['1', '2'], 'abc': '2'}

		In []: QueryMatchModel(**{"field":"ab", "logic":{'anyOf': [{'anyOf': ['1','2']}, '2']}})
		Out[]: Unsupported nesting (err 3)

		In []: QueryMatchModel(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', {'anyOf': ['1','2']}]}})
		Out[]: Unsupported nesting (err 3)

		In []: QueryMatchModel(**{"field":"ab", "logic":{'allOf': [{'allOf': ['1','2']}, '3', '1', '2']}})
		Out[]: Unsupported nesting (err 5)

		In []: QueryMatchModel(**{"field":"ab", "logic":{'anyOf': [{'allOf': ['1','2']}, '3', {'allOf': ['1',{'allOf':['1','2']}]}]}})
		Out[]: Unsupported nesting (err 4)
	"""


	field: str
	logic: Any

	def __init__(self, **kwargs):
		kwargs["logic"] = self.check_format(kwargs.get("logic"))
		super().__init__(**kwargs)

	def check_format(self, v: Any) -> dict[str, Any]:

		#print("--------------------------------")
		#print("QueryMatchModel: v: %s" % str(v))
		#print("QueryMatchModel: kwargs: %s" % kwargs)
		#print("QueryMatchModel: values: %s" % values)

		if type(v) is str:
			return {'anyOf': [v]}

		if type(v) is list:
			if not check_seq_inner_type(v, str):
				raise ValueError(
					"QueryMatchModel error\n" +
					"Unsupported nesting (err 1)"
				)
			return {'anyOf': v}

		if type(v) is dict:

			if len(v) != 1:
				raise ValueError(
					"QueryMatchModel error\n" +
					"Unsupported dict format %s" % v
				)

			if 'anyOf' in v:

				if not isinstance(v['anyOf'], strict_iterable):
					raise ValueError(
						"QueryMatchModel error\n" +
						"Invalid dict value type: %s. Must be a sequence" % type(v['anyOf'])
					)

				# 'anyOf' supports only a list of dicts and str
				if not check_seq_inner_type(v['anyOf'], (str, dict), multi_type=True):
					raise ValueError(
						"QueryMatchModel error\n" +
						"Unsupported nesting (err 2)"
					)

				for el in v['anyOf']:

					if isinstance(el, dict):

						if 'anyOf' in el:
							raise ValueError(
								"QueryMatchModel error\n" +
								"Unsupported nesting (err 3)"
							)

						elif 'allOf' in el:

							# 'allOf' closes nesting
							if not check_seq_inner_type(el['allOf'], str):
								raise ValueError(
									"QueryMatchModel error\n" +
									"Unsupported nesting (err 4)"
								)
						else:
							raise ValueError(
								"QueryMatchModel error\n" +
								"Unsupported dict: %s" % el
							)

			elif 'allOf' in v:

				if not isinstance(v['allOf'], strict_iterable):
					raise ValueError(
						"QueryMatchModel error\n" +
						"Invalid dict value type: %s. Must be a sequence" % type(v['anyOf']))

				# 'allOf' closes nesting
				if not check_seq_inner_type(v['allOf'], str):
					raise ValueError(
						"QueryMatchModel error\n" +
						"Unsupported nesting (err 5)"
					)
			else:
				raise ValueError(
					"QueryMatchModel error\n" +
					"Invalid dict key (only 'anyOf' and 'allOf' allowed)"
				)

		return v
