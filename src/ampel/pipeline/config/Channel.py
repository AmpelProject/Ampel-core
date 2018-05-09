#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/Channel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.03.2018
# Last Modified Date: 03.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce

class Channel:
	"""
	"""

	def __init__(self, doc_channel, source=None):
		"""
		doc_channel: dict instance containing channel configrations
		"""

		self.version = doc_channel['version']
		self.doc_channel = doc_channel
		self.name = doc_channel['_id']

		if source is not None:
			self.set_source(source)


	def get_name(self):
		""" """
		return self.name


	def get_version(self):
		""" """
		return self.version

	
	def get_channel_doc(self):
		""" """
		return self.doc_channel

	
	def set_source(self, source):
		""" """
		if source not in self.doc_channel['sources']:
			raise NameError("Unknown source: %s" % source)
		self.doc_source = self.doc_channel['sources'][source]


	def get_source_doc(self, source=None):
		""" """
		if source is None:
			doc_source = getattr(self, "doc_source", None)
			if doc_source is None:
				raise ValueError("Please use method set_source before calling get_config")
		else:
			if source not in self.doc_channel['sources']:
				raise NameError("Unknown source: %s" % source)
			doc_source = self.doc_channel['sources'][source]

		return doc_source


	def get_config(self, param_name, source=None):
		""" """
		doc_source = self.get_source_doc(source)
		return reduce(dict.get, param_name.split("."), doc_source)
		

	def get_custom_attr(self, name):
		""" 
		Bad practice, will try to come up with something nicer later
		"""
		return getattr(self, name, None)


	def set_custom_attr(self, name, value):
		""" 
		Bad practice, will try to come up with something nicer later
		"""
		setattr(self, name, value)
