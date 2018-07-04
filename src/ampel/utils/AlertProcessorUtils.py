#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/utils/AlertProcessorUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.07.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import importlib

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.alerts.DirAlertLoader import DirAlertLoader

class AlertProcessorUtils:


	@staticmethod
	def process_alert_folder(
		alert_processor, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", 
		extension="*.avro", serialization="avro", source="ZTFIPAC", 
		max_entries=None, console_logging=True
	):
		"""
		Process alerts in a given directory (using ampel.pipeline.t0.AlertFileList)

		Parameters:
		alert_processor: instance of ampel.pipeline.t0.AlertProcessor
		base_dir: input directory where alerts are stored
		extension: extension of alert files (default: *.avro. Alternative: *.json)
		max_entries: limit number of files loaded 
		  max_entries=5 -> only the first 5 alerts will be processed
		
		alert files are sorted by date: sorted(..., key=os.path.getmtime)
		"""

		# Container class allowing to conveniently iterate over local avro files 
		alert_loader = DirAlertLoader(alert_processor.logger)
		alert_loader.set_folder(base_dir)
		alert_loader.set_extension(extension)

		if max_entries is not None:
			alert_loader.set_max_entries(max_entries)
		
		if AmpelConfig.get_config('global.sources.%s' % source) is None:
			raise ValueError("Unknown source %s, please check your config" % source)

		alert_processor.logger.info("Processing files in folder: %s" % base_dir)

		# Instantiate class shaping alert dicts
		shaper_class_full_path = AmpelConfig.get_config(
			'global.sources.%s.alerts.processing.shape' % source
		)
		shaper_module = importlib.import_module(shaper_class_full_path)
		shaper_class = getattr(shaper_module, shaper_class_full_path.split(".")[-1])
		alert_shaper = shaper_class(alert_processor.logger)

		als = AlertSupplier(alert_loader, alert_shaper, serialization=serialization)
		ret = AlertProcessor.iter_max
		count = 0

		while ret == AlertProcessor.iter_max:
			ret = alert_processor.run(als, console_logging)
			count += ret

		return count
