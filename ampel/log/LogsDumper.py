#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/log/LogsDumper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.03.2021
# Last Modified Date: 17.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, json
from typing import Dict, Optional, Sequence, IO
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.log.LogFlag import LogFlag
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LightLogRecord import LightLogRecord
from ampel.base.AmpelFlexModel import AmpelFlexModel
from ampel.util.pretty import prettyjson


class LogsDumper(AmpelFlexModel):
	""" later """

	out: Optional[str] = None
	to_json: Optional[bool] = False
	to_pretty_json: Optional[bool] = False
	date_format: Optional[str] = None
	id_mapper: Optional[AbsIdMapper] = None
	datetime_key: str = '_id'
	resolve_flag: bool = True
	main_separator: str = ' '
	extra_separator: str = ' '


	def __init__(self, **kwargs) -> None:
		self._flag_strings: Dict = {}
		super().__init__(**kwargs)


	def process(self, log_entries: Sequence[Dict]):

		fd = open(self.out, "w") if self.out else sys.stdout

		if self.to_json or self.to_pretty_json:
			self.write_json(fd, log_entries)
		else:
			self.write_txt(fd, log_entries)

		fd.flush()

		if self.out:
			fd.close()


	def write_json(self, fd: IO, log_entries: Sequence[Dict]) -> None:

		func = json.dumps if self.to_json else prettyjson

		fd.write("[\n")
		buf = ""
		overwrite_pkey = self.datetime_key != '_id'

		for el in log_entries:
	
			# ObjectId is not json serializable
			# (and not interesting when another field contains a timestamp)
			if overwrite_pkey:
				el['_id'] = el[self.datetime_key]
				del el[self.datetime_key]

			if self.date_format:
				el['_id'] = el['_id'].strftime(self.date_format)

			if 's' in el and self.id_mapper:
				el['s'] = self.id_mapper.to_ext_id(el['s'])

			if isinstance(el['f'], LogFlag):
				if el['f'] not in self._flag_strings:
					self._flag_strings[el['f']] = str(el['f']).replace("LogFlag.", "")
				el['f'] = self._flag_strings[el['f']]

			fd.write(buf)
			buf = func(el) + ",\n" # type: ignore[operator]

		fd.write(buf[:-2] + "\n]\n")


	def write_txt(self, fd: IO, log_entries: Sequence[Dict]) -> None:

		for el in log_entries:

			out = el[self.datetime_key].strftime(self.date_format) if self.date_format else el[self.datetime_key]

			if isinstance(el['f'], LogFlag):
				if el['f'] not in self._flag_strings:
					self._flag_strings[el['f']] = self.main_separator + str(el['f']).replace("LogFlag.", "")
				out += self._flag_strings[el['f']]
			else:
				out += self.main_separator + str(el['f'])

			suffix = [f"run={el['r']}"]

			if 's' in el:
				if self.id_mapper:
					suffix.append(f"stock={self.id_mapper.to_ext_id(el['s'])}")
				else:
					suffix.append(f"stock={el['s']}")

			if 'c' in el:
				suffix.append(f"channel={el['c']}")

			if 'a' in el:
				suffix.append(f"alert={el['a']}")

			if (e := el.get('e')):
				suffix = [f'{k}={e[k]}' for k in e]

			if 'n' in el:
				suffix.append("new")

			if suffix:
				out += self.main_separator + f'[{self.extra_separator.join(suffix)}]'

			if el.get('m'):
				out += self.main_separator + el['m']

			fd.write(out + "\n")


	def log_entries(self, log_entries: Sequence[Dict], logger: AmpelLogger) -> None:
		"""
		Unsure when this could ever be required but it's there just in case
		"""

		for el in log_entries:

			record = LightLogRecord(name=0, levelno=el['f'], msg=el.get('m'))
			record.extra = el['e'] if 'e' in el else {}
			record.extra['run'] = el['r']

			if 'c' in el:
				record.channel = el['c']

			if 's' in el:
				record.stock = el['s']

			if 'a' in el:
				record.extra['alert'] = el['a']

			if 'n' in el:
				record.extra['new'] = True

			logger.handle(record)
