#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/T3BufferTextExporter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2021
# Last Modified Date: 22.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from json import dumps
from io import TextIOWrapper
from itertools import islice
from datetime import datetime
from typing import Optional, Union, TextIO, Generator, List

from ampel.t3.stage.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.content.T3Record import T3Record
from ampel.util.serialize import walk_and_encode
from ampel.util.pretty import prettyjson

dsi = dict.__setitem__
ufts = datetime.utcfromtimestamp


class T3BufferTextExporter(AbsT3Stager):
	"""
	Exports AmpelBuffer instances as BSON into file
	"""

	fd: Union[TextIOWrapper, TextIO]
	raise_exc: bool = True
	close_fd: bool = True
	update_journal: bool = False
	verbose: bool = True
	pretty: bool = False
	id_mapper: Optional[AbsIdMapper] = None
	human_times: bool = True
	chunk_size: int = 200
	

	def stage(self, gen: Generator[AmpelBuffer, None, None]) -> Optional[Union[T3Record, List[T3Record]]]:

		func = prettyjson if self.pretty else dumps

		# Shortcuts
		fd = self.fd
		id_mapper = self.id_mapper
		human_times = self.human_times
		verbose = self.verbose
		convert_timestamps = self.convert_timestamps
		first = True

		fd.write('[\n')

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
					self.logger.info(f"Writing content (id: {el['id']})") # type: ignore
				fd.write(func(el)) # type: ignore
				first = False

			for el in data:
				fd.write(",\n")
				if id_mapper:
					el['id'] = id_mapper.to_ext_id(el['id'])
				if human_times:
					convert_timestamps(el)
				if verbose:
					self.logger.info(f"Writing content (id: {el['id']})") # type: ignore
				fd.write(func(el)) # type: ignore

		fd.write('\n]\n')

		if self.close_fd:
			fd.flush()
			fd.close()

		return None


	def convert_timestamps(self, ab: AmpelBuffer) -> None:
		try:
			if 'stock' in ab:
				c = ab['stock']['created'] # type: ignore
				m = ab['stock']['modified'] # type: ignore
				j = ab['stock']['journal'] # type: ignore
				for k in c:
					dsi(c, k, ufts(c[k]).isoformat())
				for k in m:
					dsi(m, k, ufts(m[k]).isoformat())
				for ell in j:
					dsi(ell, 'ts', ufts(ell['ts']).isoformat()) # type: ignore[arg-type]
			if (x := ab.get('t1')):
				for ell in x:
					dsi(ell, 'added', ufts(ell['added']).isoformat()) # type: ignore[arg-type]
			if (y := ab.get('t2')):
				for t2 in y:
					for ell in t2['journal']:
						dsi(ell, 'ts', ufts(ell['ts']).isoformat()) # type: ignore[arg-type]
		except Exception:
			import traceback, sys
			traceback.print_exc()
			print(ab)
			sys.exit("Exception occured")

	# Mandatory
	def get_tags(self):
		return None

	# Mandatory
	def get_codes(self):
		return None
