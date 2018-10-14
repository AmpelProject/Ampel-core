#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/LogicSchemaIterator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.10.2018
# Last Modified Date: 13.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class LogicSchemaIterator:
	
	"""
	.. sourcecode:: python\n
	
		for schema in (a,b,c,d):
		print("Schema: %s" % schema)
		for i, el in enumerate(LogicSchemaIterator(schema)):
			print("Iteration %i: %s" % (i+1, el))

		Schema: 'a'
		Iteration 1: a
		Schema: {'anyOf': ['a', 'b', 'c']}
		Iteration 1: a
		Iteration 2: b
		Iteration 3: c
		Schema: {'allOf': ['a', 'b', 'c']}
		Iteration 1: {'allOf': ['a', 'b', 'c']}
		Schema: {'anyOf': [{'allOf': ['a', 'b']}, 'c']}
		Iteration 1: {'allOf': ['a', 'b']}
		Iteration 2: c
		Schema: {'anyOf': [{'allOf': ['a', 'b']}, {'allOf': ['a', 'c']}, 'd']}
		Iteration 1: {'allOf': ['a', 'b']}
		Iteration 2: {'allOf': ['a', 'c']}
		Iteration 3: d
	"""
	
	def __init__(self, arg, in_type=(str, int)):
		""" """
		self.arg = arg
		
		if type(arg) in in_type:
			self.values = [arg]
		elif isinstance(arg, dict):
			if "anyOf" in arg:
				self.values = arg['anyOf']
			elif 'allOf' in arg:
				self.values = [arg]
			else:
				raise ValueError("Unsupported format")
				
		self.count = 0
		self.limit = len(self.values)
 
	def __iter__(self):
		""" """
		return self
 
	def __next__(self):
		""" """
		try:
			return self.values[self.count]
		except IndexError:
			raise StopIteration
		finally:
			self.count += 1
