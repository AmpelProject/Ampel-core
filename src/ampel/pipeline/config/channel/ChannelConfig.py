#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/ChannelConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 11.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources
from pydantic import BaseModel, validator
from typing import List, Sequence, Any, Union, Tuple
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.config.channel.StreamConfig import StreamConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.config.ReadOnlyDict import ReadOnlyDict

@gendocstring
class ChannelConfig(AmpelModelExtension):
	"""
	Config holder for AMPEL channels

	Note: pydantic does not support typing.Sequence
	https://github.com/samuelcolvin/pydantic/issues/185
	We are thus forced to use Union[List, Tuple]
	"""
	channel: Union[int, str]
	active: bool = True
	author: str = "Unspecified"
	sources: Union[List[StreamConfig], Tuple[StreamConfig]]
	t3Supervise: Union[None, List[T3TaskConfig], Tuple[T3TaskConfig]] = None

	def __init__(self, **values):

		# We allow - for convenience - sources or t3Supervise to be defined as single dicts.
		# A cast into sequence (tuple) is necessary in this case (validator cast_to_tuple). 
		# Since a such cast modifies input, a shallow dict copy is necessary.
		if isinstance(values['sources'], dict) or isinstance(values.get('t3Supervise'), dict):
			super().__init__(**dict(values)) # shallow copy
		else:
			super().__init__(**values)


	@classmethod
	def create(cls, tier="all", **values):
		"""
		:param tier: at which tier level the returned ChannelConfig will be used. \
		Possible values are: 'all', 0, 3. \
		Less checks are performed when restricting tier to 0 or 3 which yields  \
		a lighter and thus quicker ChannelConfig loading procedure. For example, \
		with tier=0, T3 units existence or T3 run configurations are not checked.
		"""
		if tier == "all":
			if hasattr(StreamConfig, '__tier__'):
				delattr(StreamConfig, '__tier__')
		else:
			setattr(StreamConfig, '__tier__', tier)
			setattr(cls, '__tier__', tier)
		return cls(**values)


	@validator('sources', pre=True, whole=True)
	def validate_source(cls, sources, values, **kwargs):
		""" """

		# cast to tuple if dict
		if isinstance(sources, dict):
			return (sources, )

		# StreamConfig.__tier__ = 3/"all" is set by method create
		if hasattr(cls, "__tier__") and cls.__tier__ not in (3, "all"):

			sources = [dict(s) for s in sources]
			for source in sources:
				if source.get('t3Supervise'):
					del source['t3Supervise']
			return sources

		else:


			s = []

			# add various required config entries to channel-embedded task config 
			for source in sources:

				if source.get('t3Supervise'):

					# will raise exc if not found
					source_setup = next(
						pkg_resources.iter_entry_points(
							'ampel.pipeline.t0.sources', source.get('stream')
						), None
					).resolve()()

					# faster deep copy
					src = json.loads(json.dumps(source))
					for el in AmpelUtils.iter(src.get('t3Supervise')):
						if not el.get('task'):
							el['task'] = "%s_%s_%s" % (
								values['channel'], el['unitId'], 
								AmpelUtils.build_unsafe_dict_id(el)
							)
						trans = el.get("transients", {})
						sel = trans.get("select", {})
						sel["channels"] = values['channel']
						sel["withTags"] = {'allOf': source_setup.get_instrument_flag_names()}
						if type(sel["withTags"]['allOf']) is str:
							sel["withTags"]['allOf'] = [sel["withTags"]['allOf']]
					s.append(src)
				else:
					s.append(source)
	
			return tuple(s)


	@validator('t3Supervise', pre=True, whole=True)
	def enable_t0_partial_loading(cls, value):
		""" """
		if hasattr(cls, "__tier__") and cls.__tier__ == 0:
			return None
		return value


	def get_stream_config(self, source):
		"""
		:param str source:
		:returns: instance of ampel.pipeline.config.channel.StreamConfig
		:rtype: StreamConfig
		"""
		return next(filter(lambda x: x.stream==source, self.sources), None)
