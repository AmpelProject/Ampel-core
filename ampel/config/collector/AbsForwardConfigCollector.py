#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/AbsForwardConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 02.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Union, Optional, Sequence, Dict, Any, Type
import ampel.config.builder.FirstPassConfig as fpc # avoid circular import issue
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE


class AbsForwardConfigCollector(dict, AmpelABC, abstract=True):


	def __init__(self,
		# Forward reference type hint to avoid cyclic import issues
		root_config: 'fpc.FirstPassConfig',
		conf_section: str,
		target_collector_type: Type,
		logger: Optional[AmpelLogger] = None,
		verbose: bool = False,
	) -> None:

		self.has_error = False
		self.verbose = verbose
		self.root_config = root_config
		self.conf_section = conf_section
		self.target_collector_type = target_collector_type
		self.logger = AmpelLogger.get_logger() if logger is None else logger

		if verbose:
			self.logger.log(VERBOSE,
				f'Creating {self.__class__.__name__} collector '
				f'for config section "{conf_section}"'
			)


	def add(self,
		arg: Union[Dict[str, Any], List[Any], str],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str,
	) -> None:


		for el in [arg] if isinstance(arg, (dict, str)) else arg:

			path_elements = self.get_path(el, dist_name, version, register_file)
			if not path_elements:
				self.error(f' Follow-up error: could not identify routing for {el}')
				continue

			d = self.root_config
			for path in path_elements:
				d = d[path]

			if not isinstance(d, self.target_collector_type):
				self.error(
					f'Routing destination must be an instance of {self.target_collector_type}'
				)
				self.error(
					f'Type of config element with path '
					f'{".".join(str(x) for x in path_elements)}: {type(d)}'
				)
				return

			d.add(el, dist_name, version, register_file) # type: ignore


	@abstractmethod
	def get_path(self,
		arg: Union[Dict[str, Any], str],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str,
	) -> Optional[Sequence[Union[int, str]]]:
		...


	def error(self, msg: str, exc_info: Optional[Any] = None) -> None:
		self.logger.error(msg, exc_info=exc_info)
		self.has_error = True
