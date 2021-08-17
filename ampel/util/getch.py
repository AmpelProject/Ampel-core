#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/getch.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.06.2021
# Last Modified Date: 24.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class _Getch:
	"""
	Gets a single character from standard input.
	Inspired by https://stackoverflow.com/questions/510357/how-to-read-a-single-character-from-the-user
	"""
	def __init__(self, impl) -> None:
		self.impl = impl

	def __call__(self) -> bool:
		char = self.impl()
		# CTRL-C or q or ESC
		if char == '\x03' or char == 'q' or ord(char) == 27:
			return True
		elif char == '\x04':
			raise EOFError
		return False


class _GetchUnix:

	def __call__(self):
		fd = sys.stdin.fileno()
		old_settings = termios.tcgetattr(fd)
		try:
			tty.setraw(sys.stdin.fileno())
			ch = sys.stdin.read(1)
		finally:
			termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
		return ch


class _GetchWindows:

	def __call__(self):
		return msvcrt.getch()


try:
	import msvcrt
	getch = _Getch(_GetchWindows())
except ImportError:
	import sys, tty, termios
	getch = _Getch(_GetchUnix())
