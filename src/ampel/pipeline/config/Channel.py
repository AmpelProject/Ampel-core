#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/Channel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.03.2018
# Last Modified Date: 04.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.common.AmpelUtils import AmpelUtils

class Channel:
	"""
	"""

	def __init__(self, chan_name, chan_doc, source=None):
		"""
		chan_doc: dict instance containing channel configurations
		"""

		self.version = chan_doc['version']
		self.doc = chan_doc
		self.name = chan_name

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
		return self.doc


	def get_sources(self):
		""" """
		return self.doc['sources']


	def has_source(self, source):
		""" """
		return source in self.doc['sources']

	
	def set_source(self, source):
		""" """
		if source not in self.doc['sources']:
			raise NameError("Unknown source: %s" % source)
		self.doc_source = self.doc['sources'][source]


	def get_source_doc(self, source=None):
		""" """
		if source is None:
			doc_source = getattr(self, "doc_source", None)
			if doc_source is None:
				raise ValueError("Please use method set_source before calling get_config")
		else:
			if source not in self.doc['sources']:
				raise NameError("Unknown source: %s" % source)
			doc_source = self.doc['sources'][source]

		return doc_source


	def get_config(self, param_name, source=None):
		""" """
		return AmpelUtils.get_by_path(self.get_source_doc(source), param_name)


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
