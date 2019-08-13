#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/alerts/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 24.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class AlertSupplier:
	"""
	Iterable class that for each alert yielded by the provided alert_loader, 
	returns a dict in a format that the AMPEL AlertProcessor understands.
	It is essentially made of two parts:
	- An alert loader that returns either bytes, file-like objects or dicts
	- An alert shaper that shapes loaded dicts.
	"""
 
	def __init__(self, alert_loader, shape_func, serialization=None):
		"""
		:param Iterable alert_loader: iterable that returns alerts content either as:
		- dict instances, in which case, no deserialization is necessary
		- or as file-like objects / bytes, in which case deserialization is required
		:param shape_func: function that shapes a dict into a form compatible with ampel
		:param str serialization: (optional) If the alert_loader returns bytes/file_like objects, 
		deserialization is required to turn them into dicts. 
		Currently supported built-in deserialization: 'avro' or 'json'. 
		If you need other deserialization: 
			- Either implement the deserialization in your alert_loader (that will return dicts)
			- Or use the method set_deserialize_func(<method>) with an adequate method.
		"""

		self.alert_loader = alert_loader
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
				import fastavro
				def avro_next(self):
					return self.shape(
						next(
							fastavro.reader(
								next(self.alert_loader)
							)
						)
					)
				type(self).__next__ = avro_next

			else:
				raise NotImplementedError(
					"Deserialization of %s not implemented" % serialization
				)


	def set_deserialize_func(self, deserialize_func):
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


	def __iter__(self):
		return self

	
	def __next__(self):
		"""
		:returns: a dict with a format that the AMPEL AlertProcessor understands 
		:raises StopIteration: when the alert_loader dries out.
		"""
		return self.shape(
			next(self.alert_loader)
		)
