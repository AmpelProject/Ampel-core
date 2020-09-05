#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/t3/ScheduleEvaluator.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import ast

class ScheduleEvaluator(ast.NodeVisitor):
	"""
	Safely evaluate scheduling lines of the form\n
	- `every(10).minutes`
	- `every().hour``
	- `every().day.at('10:30')`
	- `every().monday`
	- `every().wednesday.at('13:15')`
	
	Allows literal numbers, strings, calling member functions of schedule.Scheduler 
	"""
	def __call__(self, scheduler, line):
		self._scheduler = scheduler
		elem = ast.parse(line).body[0]
		return self.visit(elem)

	def generic_visit(self, node):
		raise ValueError("Illegal operation {}".format(type(node)))
	
	def visit_Constant(self, node):
		return node.n
	
	def visit_Name(self, node):
		return node.id
	
	def visit_Attribute(self, node):
		return getattr(self.visit(node.value), node.attr)
	
	def visit_Call(self, node):
		args = [self.visit(arg) for arg in node.args]
		func = self.visit(node.func)
		if isinstance(func, str):
			func = getattr(self._scheduler, func)
		return func(*args)
	
	def visit_Expr(self, node):
		return self.visit(node.value)
