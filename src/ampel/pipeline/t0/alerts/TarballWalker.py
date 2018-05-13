#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/TarballWalker.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : 14.02.2019
# Last Modified Date: 13.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import tarfile
from ampel.abstract.AbsAlertLoader import AbsAlertLoader


class TarballWalker(AbsAlertLoader):
	"""
	"""

	def __init__(self, tarpath, start=0, stop=None):
		"""
		"""
		self.start = start
		self.stop = stop
		self.tarpath = tarpath


	def load_alerts(self):

		count = -1

		with open(self.tarpath, 'rb') as tar_file:

			archive = tarfile.open(fileobj=tar_file, mode='r:gz')

			for fileobj in self._walk(archive):

				count += 1

				if count < self.start:
					continue
				elif self.stop is not None and count > self.stop:
					break

				yield fileobj
				self.start +=1


	def _walk(self, archive):
		"""
		"""
		for info in archive:
			if info.isfile():
				fo = archive.extractfile(info)
				if info.name.endswith('.avro'):
					yield fo
				elif info.name.endswith('.tar.gz'):
					yield from self._walk(tarfile.open(fileobj=fo, mode='r:gz'))
