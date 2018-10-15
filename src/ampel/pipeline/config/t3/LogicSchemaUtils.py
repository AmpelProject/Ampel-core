#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/pipeline/config/t3/LogicSchemaUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.10.2018
# Last Modified Date: 13.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.t3.LogicSchemaIterator import LogicSchemaIterator
from ampel.pipeline.config.t3.AllOf import AllOf
from ampel.pipeline.config.t3.AnyOf import AnyOf

class LogicSchemaUtils:
	"""
	"""

	@staticmethod
	def iter(arg):
		"""
		Please see :obj:`LogicSchemaIterator <ampel.pipeline.config.t3.LogicSchemaIterator>` \
		docstring for more info
		"""
		return LogicSchemaIterator(arg)

	
	@staticmethod
	def reduce_to_set(arg, in_type=(str, int)):
		"""
		.. sourcecode:: python\n
			for schema in (a,b,c,d,e):
			    print("Schema: %s" % schema)
			    print("Reduced set: %s" % LogicSchemaUtils.reduce_to_set(schema))
			
			Schema: 'a'
			Reduced set: {'a'}
			Schema: {'anyOf': ['a', 'b', 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'allOf': ['a', 'b', 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'anyOf': [{'allOf': ['a', 'b']}, 'c']}
			Reduced set: {'b', 'a', 'c'}
			Schema: {'anyOf': [{'allOf': ['a', 'b']}, {'allOf': ['a', 'c']}, 'd']}
			Reduced set: {'d', 'b', 'a', 'c'}
		"""
		
		if type(arg) in in_type:
			return {arg}

		if type(arg) in (AllOf, AnyOf):
			arg = arg.dict()

		if isinstance(arg, dict):
			if "anyOf" in arg:
				s = set()
				for el in arg['anyOf']:
					if type(el) in in_type:
						s.add(el)
					elif isinstance(el, dict):
						for ell in next(iter(el.values())):
							s.add(ell)
					else:
						raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (1)")
				return s
			elif 'allOf' in arg:
				return set(arg['allOf'])
			else:
				raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (2)")
		else:
			raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported type: %s" % type(arg))


	@staticmethod
	def allOf_to_enum(arg, ClassFlag):
		"""
		.. sourcecode:: python\n
		In []: LogicSchemaUtils.allOf_to_enum({'allOf': ['INST_ZTF', 'HAS_ERROR']}, TransientFlags)
		Out[]: <TransientFlags.HAS_ERROR|INST_ZTF: 1048577>
		"""
		if "allOf" in arg:
			ret = ClassFlag[arg['allOf'][0]]
			for el in arg['allOf'][1:]:
				ret |= ClassFlag[el]
			return ret
		else:
			raise ValueError("Unsupported format")
