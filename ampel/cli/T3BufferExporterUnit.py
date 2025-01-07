#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/T3BufferExporterUnit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.08.2022
# Last Modified Date:  17.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator

from pydantic import TypeAdapter

from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.cli.export import bin_export, get_fd, txt_export
from ampel.cli.T3BufferExporterStager import TextExportOptions
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.struct.T3Store import T3Store
from ampel.struct.UnitResult import UnitResult
from ampel.types import T3Send, UBson
from ampel.view.SnapView import SnapView


class T3BufferExporterUnit(AbsT3Unit[SnapView]):

	# None means stdout
	fd: None | str = None
	binary: bool = False
	verbose: bool = True
	id_mapper: None | AbsIdMapper = None
	txt_options: TextExportOptions = TextExportOptions()


	def process(self,
		gen: Generator[SnapView, T3Send, None],
		t3s: T3Store
	) -> UBson | UnitResult:

		fd, close_fd = get_fd(self.binary, self.fd, True)

		if self.binary:
			bin_export(
				fd, # type: ignore[arg-type]
				self.serialize_views(gen), self.id_mapper, close_fd,
				logger = self.logger if self.verbose else None
			)

		else:
			txt_export(
				fd, # type: ignore[arg-type]
				self.serialize_views(gen), self.id_mapper,
				**self.txt_options.dict(), close_fd = close_fd,
				logger = self.logger if self.verbose else None
			)
		
		return None


	def serialize_views(self,
		gen: Generator[SnapView, T3Send, None],
	) -> Generator[AmpelBuffer, None, None]:
		for sv in gen:
			yield serialize(sv)

serialize = TypeAdapter(SnapView).dump_python
