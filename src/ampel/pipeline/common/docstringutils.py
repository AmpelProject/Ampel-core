#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/docstringutils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 02.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import inspect

def gendocstring(klass):
	"""
	=============================================================================
	Decorator for pydantic BaseModel child classes and python 3.7 dataclasses.
	-> Automatically generates doctring based on class members (makes required 
	variables including type hinting avail in docstring)
	=============================================================================
	Ipython warning: the module 'inspect' does not work with classes defined 
	within ipython. There is no pblm with classes stored in files.
	=============================================================================
	
	Code example:
	~~~~~~~~~~~~~
	
		from pydantic import BaseModel
		from ampel.pipeline.common.docstringutils import gendocstring
	
		@gendocstring
		class MyConfig(BaseModel):
			my_str: str
			my_int: int = 0
	
		@gendocstring
		class MyConfig2(BaseModel):
			\"\"\"
			Existing docstrings will be preserved
			\"\"\"
			my_str2: str
			my_int2: int = 0

	Generated docstrings:
	~~~~~~~~~~~~~~~~~~~~~
	
		In []: print(MyConfig__doc__)
		Out []:
		===================
		Required arguments: 
		  my_str: str
		  my_int: int = 0
		===================
	
		In []: print(MyConfig2.__doc__)
		Out []:
		=====================================
		Existing docstrings will be preserved
		-------------------------------------
		Required arguments: 
		  my_str2: str
		  my_int2: int = 0
		=====================================
	"""
	out_doc = []
	exisiting_doc = []
	in_doc = inspect.getdoc(klass)

	if in_doc:
		for el in in_doc.split('\n'):
			sel = el.strip()
			if not sel:
				continue
			exisiting_doc.append(sel)

	for el in inspect.getsource(klass).split('\n'):
		sel = el.strip()
		if not sel or "class " in sel or "\"\"\"" in sel or sel in exisiting_doc:
			continue
		if '@validator' in sel:
			break
		out_doc.append("  "+sel)

	max_len = max([len(el) for el in out_doc+exisiting_doc]+[19])
	klass.__doc__ = "="*max_len+"\n"

	if exisiting_doc:
		klass.__doc__ += "\n".join(exisiting_doc) + "\n" + "-"*max_len+"\n"

	klass.__doc__ += \
		"Required arguments: \n" + \
		"\n".join(out_doc) + \
		"\n"+"="*max_len+"\n"
	
	return klass
