#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/export.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.08.2022
# Last Modified Date:  16.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys
from json import dumps
from bson import ObjectId, encode
from io import BufferedWriter, TextIOWrapper
from itertools import islice
from datetime import datetime
from typing import TextIO, BinaryIO
from collections.abc import Generator

from ampel.protocol.LoggerProtocol import LoggerProtocol
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.util.serialize import walk_and_encode
from ampel.util.pretty import prettyjson
from ampel.util.getch import getch as fgetch

dsi = dict.__setitem__
ufts = datetime.utcfromtimestamp


def txt_export(
	fd: TextIOWrapper | TextIO,
	gen: Generator[AmpelBuffer, None, None],
	id_mapper: None | AbsIdMapper = None,
	chunk_size: int = 200,
	human_times: bool = True,
	pretty: bool = False,
	getch: bool = False,
	close_fd: bool = True,
	logger: None | LoggerProtocol = None
) -> None:

	first = True
	func = prettyjson if pretty else dumps

	fd.write('[\n')

	try:

		while (data := list(islice(gen, chunk_size))):

			walk_and_encode(data)

			if first and len(data) > 0:

				data = iter(data) # type: ignore
				el = next(data) # type: ignore
				if id_mapper:
					el['id'] = id_mapper.to_ext_id(el['id'])
				if human_times:
					convert_timestamps(el)
				if logger:
					logger.info(f"Writing content (id: {el['id']})") # type: ignore
				fd.write(func(el)) # type: ignore
				first = False

			if getch and fgetch():
				fd.write('\n]\n')
				if logger:
					logger.info("Abording")
				return None

			for el in data:

				fd.write(",\n")
				if id_mapper:
					el['id'] = id_mapper.to_ext_id(el['id'])
				if human_times:
					convert_timestamps(el)
				if logger:
					logger.info(f"Writing content (id: {el['id']})") # type: ignore
				fd.write(func(el)) # type: ignore

				if getch and fgetch():
					fd.write('\n]\n')
					if logger:
						logger.info("Abording")
					return None

		fd.write('\n]\n')

	finally:

		if close_fd:
			fd.flush()
			fd.close()


def bin_export(
	fd: BufferedWriter | BinaryIO,
	gen: Generator[AmpelBuffer, None, None],
	id_mapper: None | AbsIdMapper = None,
	close_fd: bool = True,
	logger: None | LoggerProtocol = None
) -> None:

	for el in gen:
		if logger:
			logger.info(f"Writing content (id: {el['id']})") # type: ignore[str-bytes-safe]
		if id_mapper:
			el['id'] = id_mapper.to_ext_id(el['id'])
		fd.write(encode(el))

	if close_fd:
		if logger:
			logger.info("Closing file descriptor")
		fd.flush()
		fd.close()


def get_fd(
	binary: bool,
	fd: None | str | BufferedWriter | TextIO | TextIOWrapper | BinaryIO,
	close_fd: bool
) -> tuple[BufferedWriter | TextIO | TextIOWrapper | BinaryIO, bool]:

	if binary:
		if isinstance(fd, str):
			return open(fd, "wb"), True
		return fd, close_fd # type: ignore
	else:
		if fd is None:
			return sys.stdout, False
		if isinstance(fd, str):
			return open(fd, 'w'), close_fd
		return fd, close_fd # type: ignore



def convert_timestamps(ab: AmpelBuffer) -> None:

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
				if isinstance(t1doc['_id'], str) and "oid:" in t1doc['_id']: # type: ignore[typeddict-item]
					gt = ObjectId(t1doc['_id'][4:]).generation_time # type: ignore[typeddict-item]
				else:
					gt = t1doc['_id'].generation_time # type: ignore[typeddict-item]
				dsi(t1doc, 'added', gt.isoformat()) # type: ignore[arg-type, typeddict-item]
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
