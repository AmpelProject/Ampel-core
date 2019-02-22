#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/LogicSchemaIterator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.10.2018
# Last Modified Date: 21.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.t3.AllOf import AllOf
from ampel.pipeline.config.t3.AnyOf import AnyOf
from ampel.pipeline.config.t3.OneOf import OneOf

class LogicSchemaIterator:
	
	"""
	.. sourcecode:: python\n
	
		for schema in (a,b,c):
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
		Iteration 1: ['a', 'b', 'c']

		Schema: {'anyOf': [{'allOf': ['a', 'b']}, {'allOf': ['a', 'c']}, 'd']}
		Iteration 1: ['a', 'b']
		Iteration 2: ['a', 'c']
		Iteration 3: d

		'with' scenario:
			- on scalars: if matches, then pass
			- on lists: if all matches, then pass
		'without' scenario: 
			- on scalars: if matches, then discards
			- on lists: if any matches, then discards
	"""
	
	def __init__(self, arg, in_type=(str, int)):
		""" """
		self.arg = arg
		
		if type(arg) in (AllOf, AnyOf, OneOf):
			arg = arg.dict()

		if type(arg) in in_type:
			self.values = [arg]

		elif isinstance(arg, dict):
			if "anyOf" in arg:
				self.values = arg['anyOf']
			elif 'allOf' in arg or 'oneOf' in arg:
				self.values = [arg]
			else:
				raise ValueError("Unsupported format")
		else:
			raise ValueError("Unsupported type: %s" % type(arg))
				
		self.count = -1
 
	def __iter__(self):
		""" """
		return self
 
	def __next__(self):
		""" """
		try:
			self.count += 1
			if isinstance(self.values[self.count], dict):
				if 'allOf' in self.values[self.count]:
					return self.values[self.count]['allOf']
				if 'oneOf' in self.values[self.count]:
					return self.values[self.count]['oneOf']
			return self.values[self.count] 
		except IndexError:
			raise StopIteration
