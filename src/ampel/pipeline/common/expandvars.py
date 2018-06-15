#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

import os, re

def file_env(var):
	"""
	Read var from file pointed to by ${var}_FILE, or directly from var.
	"""
	if '{}_FILE'.format(var) in os.environ:
		with open(os.environ['{}_FILE'.format(var)]) as f:
			return f.read().strip()
	else:
		return os.environ[var]

def expandvars(path, get=file_env):
	"""
	Recursively expand shell variables of form $var and ${var}. Unknown variables raise
	an error. Adapted from os.posixpath.expandvars
	
	:param get: callable returning the values of environment variables
	
	"""
	if not isinstance(path, str):
		return path
	if '$' not in path:
		return path
	_varprog = re.compile(r'\$(\w+|\{[^}]*\})', re.ASCII)
	search = _varprog.search
	start = '{'
	end = '}'
	i = 0
	while True:
		m = search(path, i)
		if not m:
			break
		i, j = m.span(0)
		name = m.group(1)
		if name.startswith(start) and name.endswith(end):
			name = name[1:-1]
		try:
			value = get(name)
		except KeyError:
			i = j
			raise
		else:
			tail = path[j:]
			path = path[:i] + value
			i = len(path)
			path += tail
	return expandvars(path, get)
