#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/t3/LogicSchemaUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.10.2018
# Last Modified Date: 21.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.config.t3.LogicSchemaIterator import LogicSchemaIterator
from ampel.config.t3.AllOf import AllOf
from ampel.config.t3.AnyOf import AnyOf
from ampel.config.t3.OneOf import OneOf

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

		if type(arg) in (AllOf, AnyOf, OneOf):
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
			elif 'oneOf' in arg:
				return set(arg['oneOf'])
			else:
				raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported format (2)")
		else:
			raise ValueError("LogicSchemaUtils.reduce_to_set: unsupported type: %s" % type(arg))
