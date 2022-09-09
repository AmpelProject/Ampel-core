#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/pretty.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                Unspecified
# Last Modified Date:  06.09.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys, re, html, math
from math import isinf
from time import time
from datetime import timedelta
from contextlib import contextmanager
from ampel.protocol.LoggerProtocol import LoggerProtocol

# copied from https://stackoverflow.com/a/56497521/104668
def prettyjson(obj, indent=2, maxlinelength=80):
	"""Renders JSON content with indentation and line splits/concatenations to fit maxlinelength.
	Only dicts, lists and basic types are supported"""
	items, _ = getsubitems(obj, itemkey="", islast=True, maxlinelength=maxlinelength, level=0)
	return indentitems(items, indent, level=0)


def getsubitems(obj, itemkey, islast, maxlinelength, level):

	items = []
	is_inline = True # at first, assume we can concatenate the inner tokens into one line

	isdict = isinstance(obj, dict)
	islist = isinstance(obj, list)
	istuple = isinstance(obj, tuple)
	isbasictype = not (isdict or islist or istuple)

	# build json content as a list of strings or child lists
	if isbasictype:
		# render basic type
		keyseparator = "" if itemkey == "" else ": "
		itemseparator = "" if islast else ","
		items.append(itemkey + keyseparator + basictype2str(obj) + itemseparator)

	else:
		# render lists/dicts/tuples
		if isdict:
			opening, closing, keys = ("{", "}", iter(obj.keys()))
		elif islist:
			opening, closing, keys = ("[", "]", range(0, len(obj)))
		elif istuple:
			opening, closing, keys = ("[", "]", range(0, len(obj)))	# tuples are converted into json arrays

		if itemkey != "":
			opening = itemkey + ": " + opening
		if not islast:
			closing += ","

		itemkey = ""
		subitems = []

		# get the list of inner tokens
		for (i, k) in enumerate(keys):
			islast_ = i == len(obj) - 1
			itemkey_ = ""
			if isdict:
				itemkey_ = basictype2str(k)
			inner, is_inner_inline = getsubitems(obj[k], itemkey_, islast_, maxlinelength, level + 1)
			subitems.extend(inner)						# inner can be a string or a list
			is_inline = is_inline and is_inner_inline	 # if a child couldn't be rendered inline, then we are not able either

		# fit inner tokens into one or multiple lines, each no longer than maxlinelength
		if is_inline:
			multiline = True

			# in Multi-line mode items of a list/dict/tuple can be rendered in multiple lines if they don't fit on one.
			# suitable for large lists holding data that's not manually editable.

			# in Single-line mode items are rendered inline if all fit in one line, otherwise each is rendered in a separate line.
			# suitable for smaller lists or dicts where manual editing of individual items is preferred.

			# this logic may need to be customized based on visualization requirements:
			if (isdict):
				multiline = False
			if (islist):
				multiline = True

			if (multiline):
				lines = []
				current_line = ""

				for (i, item) in enumerate(subitems):
					item_text = item
					if i < len(inner) - 1:
						item_text = item + ","

					if len(current_line) > 0:
						try_inline = current_line + " " + item_text
					else:
						try_inline = item_text

					if (len(try_inline) > maxlinelength):
						# push the current line to the list if maxlinelength is reached
						if len(current_line) > 0:
							lines.append(current_line)
						current_line = item_text
					else:
						# keep fitting all to one line if still below maxlinelength
						current_line = try_inline

					# Push the remainder of the content if end of list is reached
					if (i == len(subitems) - 1):
						lines.append(current_line)

				subitems = lines
				if len(subitems) > 1:
					is_inline = False
			else: # single-line mode
				totallength = len(subitems) - 1   # spaces between items
				for item in subitems:
					totallength += len(item)
				if (totallength <= maxlinelength):
					str = ""
					for item in subitems:
						str += item + " "  # insert space between items, comma is already there
					subitems = [str.strip()]			   # wrap concatenated content in a new list
				else:
					is_inline = False


		# attempt to render the outer brackets + inner tokens in one line
		if is_inline:
			item_text = ""
			if len(subitems) > 0:
				item_text = subitems[0]
			if len(opening) + len(item_text) + len(closing) <= maxlinelength:
				items.append(opening + item_text + closing)
			else:
				is_inline = False

		# if inner tokens are rendered in multiple lines already, then the outer brackets remain in separate lines
		if not is_inline:
			items.append(opening)	   # opening brackets
			items.append(subitems)	  # Append children to parent list as a nested list
			items.append(closing)	   # closing brackets

	return items, is_inline


def basictype2str(obj):
	if (
		isinstance(obj, str) or
		obj.__class__.__name__ == "ObjectId" or
		(isinstance(obj, float) and isinf(obj))
	):
		return "\"" + str(obj) + "\""
	elif isinstance(obj, bool):
		return "true" if obj else "false"
	else:
		return str(obj)


def indentitems(items, indent, level):
	"""Recursively traverses the list of json lines, adds indentation based on the current depth"""
	res = ""
	indentstr = " " * (indent * level)
	for (i, item) in enumerate(items):
		if isinstance(item, list):
			res += indentitems(item, indent, level + 1)
		else:
			islast = (i == len(items) - 1)
			# no new line character after the last rendered line
			if level == 0 and islast:
				res += indentstr + item
			else:
				res += indentstr + item + "\n"
	return res

# End of prettyjson
###################

# Notebook goodies
def set_bold(s: str, match: str):

	from IPython.display import HTML # type: ignore[import]
	out = []

	for el in s.split("\n"):
		if re.match(match, el):
			out.append(f"<b>{html.escape(el)}</b>")
		else:
			out.append(html.escape(el))
	return HTML("<pre style='font-size: 13px'>" + "<br/>".join(out) + "</pre>")


@contextmanager
def out_stack():
	"""
	with out_stack():
		raise ValueError("Clean and concise")
	"""
	default_value = getattr(sys, "tracebacklimit", 1000)
	sys.tracebacklimit = 0
	print(" ")
	yield
	sys.tracebacklimit = default_value


def human_format(num, precision=2, suffixes=['', 'K', 'M', 'G', 'T', 'P']):
	"""
	In []: human_format(1000000, precision=0)
	Out[]: '1M'

	In []: human_format(30000, precision=0)
	Out[]: '30K'

	In []: human_format(123456)
	Out[]: '123.46K'
	"""
	m = int(math.log10(num) // 3)
	return f'{num/1000.0**m:.{precision}f}{suffixes[m]}'


def get_time_delta(start: float, cut: None | int = -7) -> str:
	return str(timedelta(seconds=time()-start))[:-7]


class TimeFeedback:
	"""
	Usage:
	with TimeFeedback("2d binning"):
		do_it()
	"""

	def __init__(self,
		feedback: str = "Time elapsed",
		cut: None | int = -7,
		logger: None | LoggerProtocol = None,
		enter_feedback: bool = False
	):
		self.feedback = feedback
		self.logger = logger
		self.cut = cut
		self.enter_feedback = enter_feedback

	def __enter__(self):
		self.start = time()
		if self.enter_feedback:
			self._print(
				f"Measuring time for '{self.feedback}'"
				if self.feedback != "Time elapsed"
				else "Starting time measurement"
			)
		return self

	def __exit__(self, *args):
		self._print(self.feedback + ": " + get_time_delta(self.start, self.cut))

	def _print(self, s: str) -> None:
		if self.logger:
			self.logger.info(s)
		print(s)
