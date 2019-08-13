#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/AlertFileList.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging

class AlertFileList:

	def __init__(self, logger=None):
		""" """

		self.files = []
		self.min_index = None
		self.max_index = None
		self.max_entries = None
		self.folder = "/Users/hu/Documents/ZTF/IPAC-ZTF/ztf/src/pl/avroalerts/testprod"
		self.extension = "*.avro"
		self.logger = logging.getLogger("Ampel") if logger is None else logger


	def set_extension(self, extension):
		""" """
		self.extension = extension


	def set_folder(self, arg):
		""" """
		self.folder = arg
		self.logger.debug("Target incoming folder: " + self.folder)


	def set_index_range(self, min_index=None, max_index=None):
		""" """
		self.min_index = min_index
		self.max_index = max_index
		self.logger.debug("Min index set to: " + str(self.min_index))
		self.logger.debug("Max index set to: " + str(self.max_index))


	def set_max_entries(self, max_entries):
		""" """
		self.max_entries = max_entries
		self.logger.debug("Max entries set to: " + str(self.max_entries))


	def add_files(self, arg):
		""" """
		self.files.append(arg)
		self.logger.debug("Adding " + str(len(arg)) + " files to the list")


	def build_file_list(self):
		""" """
		self.logger.debug("Building internal file list")

		import glob, os
		all_files = sorted(glob.glob(self.folder + "/" + self.extension), key=os.path.getmtime)

		if self.min_index is not None:
			self.logger.debug("Filtering files using min_index criterium")
			out_files = []
			for f in all_files:
				if int(os.path.basename(f).split(".")[0]) >= self.min_index:
					out_files.append(f)
			all_files = out_files

		if self.max_index is not None:
			self.logger.debug("Filtering files using max_index criterium")
			out_files = []
			for f in all_files:
				if int(os.path.basename(f).split(".")[0]) <= self.max_index:
					out_files.append(f)
			all_files = out_files

		if self.max_entries is not None:
			self.logger.debug("Filtering files using max_entries criterium")
			self.files = all_files[:self.max_entries]
		else:
			self.files = all_files

		self.logger.debug("File list contains " + str(len(self.files)) + " elements")


	def get_files(self):
		""" """
		if not self.files:
			self.build_file_list()

		return self.files
