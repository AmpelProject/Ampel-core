#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/LogicSchemaIterator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.10.2018
# Last Modified Date: 04.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf


class LogicSchemaIterator:
	"""
	.. sourcecode:: python\n

		for schema in (a,b,c):
			print("Schema: %s" % schema)
			for i, el in enumerate(LogicSchemaIterator(schema)):
				print("Iteration %i: %s" % (i+1, el))

		Schema: 'a'
		Iteration 1: a

		Schema: {'any_of': ['a', 'b', 'c']}
		Iteration 1: a
		Iteration 2: b
		Iteration 3: c

		Schema: {'all_of': ['a', 'b', 'c']}
		Iteration 1: ['a', 'b', 'c']

		Schema: {'any_of': [{'all_of': ['a', 'b']}, {'all_of': ['a', 'c']}, 'd']}
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

		if isinstance(arg, (AllOf, AnyOf, OneOf)):
			arg = arg.dict()

		if isinstance(arg, in_type):
			self.values = [arg]

		elif isinstance(arg, dict):
			if "any_of" in arg:
				self.values = arg['any_of']
			elif 'all_of' in arg or 'one_of' in arg:
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
				if 'all_of' in self.values[self.count]:
					return self.values[self.count]['all_of']
				if 'one_of' in self.values[self.count]:
					return self.values[self.count]['one_of']
			return self.values[self.count]
		except IndexError:
			raise StopIteration
