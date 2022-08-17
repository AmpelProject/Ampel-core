#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/T3BufferExporterStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  17.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from io import BufferedWriter, TextIOWrapper
from datetime import datetime
from typing import BinaryIO, TextIO, Any
from collections.abc import Generator

from ampel.types import Traceless
from ampel.view.T3Store import T3Store
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.core.EventHandler import EventHandler
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.cli.export import txt_export, bin_export, get_fd

dsi = dict.__setitem__
ufts = datetime.utcfromtimestamp


class TextExportOptions(AmpelBaseModel):
	chunk_size: int = 200
	human_times: bool = True
	pretty: bool = False
	getch: bool = False


class T3BufferExporterStager(AbsT3Stager):
	"""
	Exports AmpelBuffer instances as BSON or text file
	"""

	logger: Traceless[AmpelLogger]
	event_hdlr: Traceless[EventHandler]
	channel: Any

	fd: None | str | BufferedWriter | TextIO | TextIOWrapper | BinaryIO
	binary: bool
	verbose: bool = True
	close_fd: bool = True
	id_mapper: None | AbsIdMapper = None
	txt_options: TextExportOptions = TextExportOptions()


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:

		fd, close_fd = get_fd(self.binary, self.fd, self.close_fd)
		
		if self.binary:
			bin_export(
				fd, # type: ignore[arg-type]
				gen, self.id_mapper, close_fd,
				AmpelLogger.get_logger() if self.verbose else None
			)

		else:
			txt_export(
				fd, # type: ignore[arg-type]
				gen, self.id_mapper, **self.txt_options.dict(), close_fd = self.close_fd,
				logger = AmpelLogger.get_logger() if self.verbose else None
			)

		return None
