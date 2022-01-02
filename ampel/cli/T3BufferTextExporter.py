#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/T3BufferTextExporter.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  10.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from json import dumps
from io import TextIOWrapper
from itertools import islice
from datetime import datetime
from typing import TextIO
from collections.abc import Generator

from ampel.view.T3Store import T3Store
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Document import T3Document
from ampel.util.serialize import walk_and_encode
from ampel.util.pretty import prettyjson
from ampel.util.getch import getch as fgetch

dsi = dict.__setitem__
ufts = datetime.utcfromtimestamp


class T3BufferTextExporter(AbsT3Stager):
	"""
	Exports AmpelBuffer instances as BSON into file
	"""

	fd: TextIOWrapper | TextIO
	raise_exc: bool = True
	close_fd: bool = True
	update_journal: bool = False
	verbose: bool = True
	pretty: bool = False
	id_mapper: None | AbsIdMapper = None
	human_times: bool = True
	chunk_size: int = 200
	getch: bool = False


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:

		logger = AmpelLogger.get_logger()
		func = prettyjson if self.pretty else dumps

		# Shortcuts
		fd = self.fd
		id_mapper = self.id_mapper
		human_times = self.human_times
		verbose = self.verbose
		convert_timestamps = self.convert_timestamps
		first = True

		fd.write('[\n')

		try:

			while (data := list(islice(gen, self.chunk_size))):

				walk_and_encode(data)

				if first and len(data) > 0:

					data = iter(data) # type: ignore
					el = next(data) # type: ignore
					if id_mapper:
						el['id'] = id_mapper.to_ext_id(el['id'])
					if human_times:
						convert_timestamps(el)
					if verbose:
						logger.info(f"Writing content (id: {el['id']})") # type: ignore
					fd.write(func(el)) # type: ignore
					first = False

				if self.getch and fgetch():
					fd.write('\n]\n')
					logger.info("Abording")
					return None

				for el in data:

					fd.write(",\n")
					if id_mapper:
						el['id'] = id_mapper.to_ext_id(el['id'])
					if human_times:
						convert_timestamps(el)
					if verbose:
						logger.info(f"Writing content (id: {el['id']})") # type: ignore
					fd.write(func(el)) # type: ignore

					if self.getch and fgetch():
						fd.write('\n]\n')
						logger.info("Abording")
						return None

			fd.write('\n]\n')

		finally:

			if self.close_fd:
				fd.flush()
				fd.close()

		return None


	def convert_timestamps(self, ab: AmpelBuffer) -> None:

		try:

			if stock_doc := ab.get('stock'):
				for k in (ts := stock_doc['ts']):
					dsi( # type: ignore[misc]
						ts, k, {
							'tied': ufts(ts[k]['tied']).isoformat(),
							'upd': ufts(ts[k]['upd']).isoformat()
						}
					)
				for je in stock_doc['journal']:
					dsi(je, 'ts', ufts(je['ts']).isoformat()) # type: ignore[arg-type]

			if t1s := ab.get('t1'):
				for t1doc in t1s:
					dsi(t1doc, 'added', ufts(t1doc['_id'].generation_time).isoformat()) # type: ignore[arg-type, typeddict-item]
					for t1meta in t1doc['meta']:
						dsi(t1meta, 'ts', ufts(t1meta['ts']).isoformat()) # type: ignore[arg-type]

			if t2s := ab.get('t2'):
				for t2doc in t2s:
					for t2meta in t2doc['meta']:
						dsi(t2meta, 'ts', ufts(t2meta['ts']).isoformat()) # type: ignore[arg-type]

		except Exception:
			import traceback, sys
			traceback.print_exc()
			print(ab)
			sys.exit("Exception occured")
