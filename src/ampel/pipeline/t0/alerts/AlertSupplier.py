#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 14.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class AlertSupplier:
	"""
	Iterable class that for each alert yield by the provided alert_loader, 
	returns a dict featuring a format that the AMPEL AlertProcessor understands 
	"""
 
	def __init__(self, alert_loader, shape_func, serialization=None):
		"""
		:param alert_loader: loads and returns alerts file like objects. Class must be iterable.
		:param shape_func: function that shapes a dict into a form compatible with ampel
		:param serialization: (optional) either 'avro' or 'json'. Sets the corresponding 
		deserialization function used to convert file-like objects into dict
		"""

		if serialization is None:
			self.deserialize = None
		else:

			if serialization == "json":
				import json
				self.deserialize = lambda f: (json.load(f), {})

			elif serialization == "avro":
				import fastavro
				def deserialize(f):
					reader = fastavro.reader(f)
					return next(reader, None), reader.schema
				self.deserialize = deserialize

			else:
				raise NotImplementedError(
					"Deserialization of %s not implemented" % serialization
				)

		self.alert_loader = alert_loader
		self.shape = shape_func


	def set_deserialize_func(self, deserialize_func):
		"""
		Convenience method allowing to override values provided during class instantiation
		:param deserializer_func: function deserializing file_like objects into dict 
		:returns: None
		"""
		self.deserialize = deserialize_func


	def set_shape_func(self, shape_func):
		"""
		Convenience method allowing to override values provided during class instantiation
		:param shape_func: function that shapes a dict into a form compatible with ampel 
		:returns: None
		"""
		self.shape = shape_func


	def __iter__(self):
		return self

	
	def __next__(self):
		"""
		:returns: a dict with a format that the AMPEL AlertProcessor understands 
		or None if the alert_loader has dried out.
		"""
		if self.deserialize is None:
			return self.shape(
				next(self.alert_loader)
			)
		else:
			fileobj = next(self.alert_loader)
			if isinstance(fileobj, tuple): # ZIAlertFetcher does return a tuple
				fileobj, partition_id = fileobj
			else:
				partition_id = 0
			alert, schema = self.deserialize(fileobj)
			return self.shape(alert)
