#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/dispatchers/AbstractAmpelDispatcher.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 27.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import inspect
from ampel.pipeline.common.AmpelABC import AmpelABC, abstractmethod

class AbstractAmpelDispatcher(metaclass=AmpelABC):


	@abstractmethod
	def set_job_id(self, job_id):
		return


	@abstractmethod
	def dispatch(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):
		return


	def __new__(cls, *args, **kwargs):
		if cls is AbstractAmpelDispatcher:
			raise TypeError("Class AbstractAmpelDispatcher cannot be instantiated")


#	def __init_subclass__(cls):
#
#		for method_name in AbstractAmpelDispatcher.abstract_methods:
#
#			func = getattr(cls, method_name, False)
#			if func:
#				if func.__qualname__.split(".")[0] == AbstractAmpelDispatcher.__name__:
#					raise NotImplementedError(
#						"Method " + method_name  + " is not implemented"
#					)
#
#			abstract_sig = inspect.signature(getattr(AbstractAmpelDispatcher, method_name))
#			child_sig = inspect.signature(getattr(cls, method_name))
#
#			if not abstract_sig == child_sig:
#				raise NotImplementedError(
#					"Signature is wrong for method " + 
#					method_name  + 
#					", please check defined arguments"
#				)
