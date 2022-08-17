#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/debug.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                24.07.2022
# Last Modified Date:  17.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys, pdb, traceback

"""
Usage:
from ampel.util.debug import mp_breakpoint
-> mp_breakpoint()

from ampel.util.debug import trace_prints
-> trace_prints()
"""

# https://stackoverflow.com/questions/26289153/how-to-use-ipdb-set-trace-in-a-forked-process
class ForkedPdb(pdb.Pdb):
	"""
	A Pdb subclass that may be used within a multiprocessing process

	Forword: If you are not using multiprocessing, just use breakpoint()

	Usage:
	insert: "ForkedPdb().set_trace()" at the wished position
	<run your job>
	a pdb shell should pop up, type "interact" and press enter
	<do debug>
	when you are done, press CTRL-D
	you are back in the pdb shell, type "continue" and press enter
	Hope this helps
	"""

	def interaction(self, *args, **kwargs):
		_stdin = sys.stdin
		try:
			sys.stdin = open('/dev/stdin')
			pdb.Pdb.interaction(self, *args, **kwargs)
		finally:
			sys.stdin = _stdin


# https://stackoverflow.com/questions/1617494/finding-a-print-statement-in-python
class TracePrints:

	def __init__(self):    
		self.stdout = sys.stdout

	def write(self, s):
		self.stdout.write("Writing %r\n" % s)
		traceback.print_stack(file=self.stdout)


def trace_prints():
	original_stdout.flush()
	sys.stdout = TracePrints()

def stop_trace_prints():
	original_stdout.flush()
	sys.stdout = original_stdout

mp_breakpoint = ForkedPdb().set_trace
original_stdout = sys.stdout
