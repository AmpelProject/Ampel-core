#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/T3BufferBinaryExporter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2021
# Last Modified Date: 10.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import encode
from io import BufferedWriter
from typing import Optional, Union, BinaryIO, Generator

from ampel.view.T3Store import T3Store
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsIdMapper import AbsIdMapper


class T3BufferBinaryExporter(AbsT3Stager):
	"""
	Exports AmpelBuffer instances as JSON (with base64 encoded bytes values) into file
	"""

	fd: Union[BufferedWriter, BinaryIO]
	raise_exc: bool = True
	update_journal: bool = False
	close_fd: bool = True
	verbose: bool = True
	id_mapper: Optional[AbsIdMapper] = None


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> Optional[Generator[T3Document, None, None]]:

		logger = AmpelLogger.get_logger()

		# Shortcuts
		fd = self.fd
		id_mapper = self.id_mapper
		verbose = self.verbose

		for el in gen:
			if verbose:
				logger.info(f"Writing content (id: {el['id']})") # type: ignore[str-bytes-safe]
			if id_mapper:
				el['id'] = id_mapper.to_ext_id(el['id'])
			fd.write(encode(el))

		logger.info("Closing file descriptor")
		if self.close_fd:
			fd.flush()
			fd.close()

		return None
