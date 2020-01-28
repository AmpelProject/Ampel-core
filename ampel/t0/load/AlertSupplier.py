#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/load/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 04.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from io import IOBase
from typing import Iterable, Dict, Callable, Any

class AlertSupplier:
	"""
	Iterable class that for each alert yielded by the provided alert_loader, 
	returns a dict in a format that AlertProcessor understands.
	It is essentially made of two parts:
	- An alert loader that returns either bytes, file-like objects or dicts
	- An alert shaper that enforce a fixed structure for the loaded dicts
	"""

	def __init__(
		self, shape_func: Callable[[Dict[str, Any]], Dict[str, Any]], 
		serialization: str = None
	):
		"""
		:param shape_func: function that shapes a dict into a form compatible with ampel
		:param str serialization: (optional) If the alert_loader returns bytes/file_like objects, 
		deserialization is required to turn them into dicts. 
		Currently supported built-in deserialization: 'avro' or 'json'. 
		If you need other deserialization: 
		- Either implement the deserialization in your alert_loader (that will return dicts)
		- Or use the method set_deserialize_func(<method>) with an adequate method.
		"""

		self.shape = shape_func

		if serialization is not None:

			if serialization == "json":
				import json
				def json_next(self):
					return self.shape(
						json.load(
							next(self.alert_loader)
						)
					)
				type(self).__next__ = json_next
				
			elif serialization == "avro":
				from fastavro import reader
				def avro_next(self):
					return self.shape(
						next(
							reader(
								next(self.alert_loader)
							)
						)
					)
				type(self).__next__ = avro_next

			else:
				raise NotImplementedError(
					"Deserialization of %s not implemented" % serialization
				)


	def set_alert_source(self, alert_loader: Iterable[IOBase]) -> None:
		""" 
		:param alert_loader: iterable that returns alerts content 
		as as file-like objects / bytes
		"""
		self.alert_loader = alert_loader


	def set_deserialize_func(self, deserialize_func: Callable[[IOBase], Dict[str, Any]]) -> None:
		"""
		Convenience method allowing to set own deserialization function
		:param deserializer_func: function deserializing file_like objects into dict 
		:returns: None
		"""

		def custom_next(self):
			return self.shape(
				deserialize_func(
					next(self.alert_loader)
				)
			)

		type(self).__next__ = custom_next


	def ready(self) -> bool:
		""" """
		return hasattr(self, "alert_loader")


	def __iter__(self):
		""" """
		return self

	
	def __next__(self) -> Dict[str, Any]:
		"""
		:returns: a dict with a structure that AlertProcessor understands 
		:raises StopIteration: when alert_loader dries out.
		:raises AttributeError: if alert_loader was not set properly before this method is called
		"""
		return self.shape(
			next(self.alert_loader)
		)
