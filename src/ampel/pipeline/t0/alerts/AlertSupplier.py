#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/AlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.core.abstract.AbsAlertShaper import AbsAlertShaper
import time

class AlertSupplier:

 
	def __init__(self, alert_loader, alert_shaper, serialization=None, archive=None):
		"""
		:param alert_loader: loads and returns alerts file like objects. Class must be iterable.
		:param alert_shaper: reshapes dict into a form compatible with ampel
		:param serialization: (optional) either 'avro' or 'json'. Sets the corresponding 
		deserialization function used to convert file-like objects into dict
		"""

		if not issubclass(alert_shaper.__class__, AbsAlertShaper):
			raise ValueError("Second argument must be a child class of AbsAlertShaper")

		if serialization is not None:

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
		self.alert_archive = archive
		self.alert_shaper = alert_shaper
 

	def set_deserializer_func(self, deserializer_func):
		"""
		deserializer_func: function deserializing file_like objects into dict 
		"""
		self.deserialize = deserializer_func


	def __iter__(self):
		return self

	
	def __next__(self):
		"""
		"""
		if not hasattr(self, 'deserialize'):
			assert self.alert_archive is None
			return self.alert_shaper.shape(
				next(self.alert_loader)
			)
		else:
			fileobj = next(self.alert_loader)
			if isinstance(fileobj, tuple):
				fileobj, partition_id = fileobj
			else:
				partition_id = 0
			alert, schema = self.deserialize(fileobj)
			if self.alert_archive is not None:
				self.alert_archive.insert_alert(alert, schema, partition_id, int(1e6*time.time()))
			return self.alert_shaper.shape(alert)
