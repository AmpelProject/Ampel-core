#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/utils/AlertProcessorUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.07.2018
# Last Modified Date: 24.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.t0.load.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.load.DirAlertLoader import DirAlertLoader

class AlertProcessorUtils:


	@staticmethod
	def process_alert_folder(
		alert_processor, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", 
		extension="*.avro", serialization="avro", max_entries=None
	):
		"""
		Process alerts in a given directory (using ampel.pipeline.t0.AlertFileList)

		Parameters:
		:param ampel.pipeline.t0.AlertProcessor alert_processor: alert processor instance
		:param str base_dir: input directory where alerts are stored
		:param str extension: extension of alert files (default: *.avro. Alternative: *.json)
		:param max_entries: limit number of files loaded 
		  max_entries=5 -> only the first 5 alerts will be processed
		
		alert files are sorted by date: sorted(..., key=os.path.getmtime)
		"""

		# Container class allowing to conveniently iterate over local avro files 
		alert_loader = DirAlertLoader(alert_processor.logger)
		alert_loader.set_folder(base_dir)
		alert_loader.set_extension(extension)

		if max_entries is not None:
			alert_loader.set_max_entries(max_entries)
		
		alert_processor.logger.info("Processing files in folder: %s" % base_dir)

		ret = AlertProcessor.iter_max
		count = 0

		while ret == AlertProcessor.iter_max:
			ret = alert_processor.run(alert_loader)
			count += ret

		return count
