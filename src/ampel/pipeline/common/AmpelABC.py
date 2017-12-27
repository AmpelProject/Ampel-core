#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelABC.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.12.2017
# Last Modified Date: 27.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import inspect

def abstractmethod(func):
	AmpelABC.abstract_methods.add(func.__name__)
	return func


class AmpelABC(type):

	abstract_methods = set()


	@staticmethod
	def forbid_new(name):
		def __new__(cls, *args, **kwargs):
			raise TypeError("Class "+ name + " cannot be instantiated")
		return __new__


	@staticmethod
	def generate__init_subclass__(mc):

		def __init_subclass__(cls):

			for method_name in cls.abstract_methods:
	
				func = getattr(cls, method_name, False)
				if func:
					if func.__qualname__.split(".")[0] == mc.__name__:
						raise NotImplementedError(
							"Method " + method_name  + " is not implemented"
						)
	
				abstract_sig = inspect.signature(getattr(mc, method_name))
				child_sig = inspect.signature(getattr(cls, method_name))
	
				if not abstract_sig == child_sig:
					raise NotImplementedError(
						"Signature is wrong for method " + 
						method_name  + 
						", please check defined arguments"
					)

		return __init_subclass__


	def __new__(metacls, name, bases, d):

		if len(AmpelABC.abstract_methods) > 0:
			d['abstract_methods'] = []
			for el in AmpelABC.abstract_methods:
				d['abstract_methods'].append(el)
			AmpelABC.abstract_methods = set()
			d['__init_subclass__'] = AmpelABC.generate__init_subclass__(
				type.__new__(metacls, name, bases, d)
			)

		return type.__new__(metacls, name, bases, d)
