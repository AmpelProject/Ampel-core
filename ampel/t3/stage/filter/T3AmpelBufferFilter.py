#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/filter/T3AmpelBufferFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from typing import Sequence, Literal, List, Union, get_args, Iterable
from ampel.types import ChannelId
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.struct.AmpelBuffer import AmpelBuffer, BufferKey
from ampel.model.UnitModel import UnitModel
from ampel.model.StrictModel import StrictModel
from ampel.core.UnitLoader import UnitLoader
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsT3Filter import AbsT3Filter
from ampel.aux.filter.AbsLogicOperatorFilter import AbsLogicOperatorFilter


channel_id = get_args(ChannelId) # type: ignore[misc]


class FilterBlock(StrictModel):

	data: Union[BufferKey, Literal['journal']]
	filter: AbsLogicOperatorFilter
	include: bool = True

	def __init__(self, **kwargs):
		StrictModel.__init__(self, **kwargs)
		typ = "include" if self.include else "exclude"
		self.descr = f"{self.filter.__class__.__name__}[target={self.data}, on_match={typ}]"


class T3AmpelBufferFilter(AbsT3Filter):

	class FilterModel(StrictModel):
		data: Union[BufferKey, Literal['journal']]
		filter: UnitModel
		on_match: Literal['include', 'exclude'] = 'include'

	logger: AmpelLogger
	filters: Sequence[FilterModel] = []
	channel: Union[None, ChannelId, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]] = None

	def __init__(self, **kwargs):

		# Kulanz
		if 'filters' in kwargs and not isinstance(kwargs['filters'], collections.abc.Sequence):
			kwargs['filters'] = [kwargs['filters']]

		super().__init__(**kwargs)
		self.filter_blocks: List[FilterBlock] = []

		for f in self.filters:

			self.filter_blocks.append(
				FilterBlock(
					data = f.data,
					filter = UnitLoader.new_aux_unit(f.filter),
					include = f.on_match == "include"
				)
			)


	def filter(self, it: Iterable[AmpelBuffer]) -> Sequence[AmpelBuffer]:

		debug = self.logger.verbose > 1
		ret: List[AmpelBuffer]

		if self.channel:

			if isinstance(self.channel, channel_id):
				ret = [
					ab for ab in it
					if ab['stock']['channel'] == self.channel or # type: ignore
					self.channel in ab['stock']['channel'] # type: ignore
				]


			elif isinstance(self.channel, AnyOf):
				ret = []
				for ab in it:
					if 'stock' not in ab or not ab['stock']:
						continue
					for chan in self.channel.any_of:
						if ab['stock']['channel'] == chan or chan in ab['stock']['channel']: # type: ignore
							ret.append(ab)
							break

			elif isinstance(self.channel, AllOf):
				ret = []
				for ab in it:
					if 'stock' not in ab or not ab['stock']:
						continue
					if all([chan in ab['stock']['channel'] for chan in self.channel.all_of]): # type: ignore
						ret.append(ab)

			elif isinstance(self.channel, OneOf):
				ret = []
				for ab in it:
					if 'stock' not in ab or not ab['stock']:
						continue
					if len(ab['stock']['channel']) == 1: # type: ignore
						for chan in self.channel.one_of:
							if ab['stock']['channel'][0] == chan: # type: ignore
								ret.append(ab)
								break
			else:
				raise ValueError("Unrecognized parameter")

			# if debug and len(ret) != len(it):
			#	self.logger.debug(f"AmpelBuffer stock channel filter: in: {len(it)}, out: {len(ret)}")

		else:
			ret = list(it)

		for fb in self.filter_blocks:

			jfilter = fb.data == 'journal'

			# Using the old trick of iterating backwards to remove list element while iterating over list
			for i in range(len(ret) - 1, -1, -1):

				abuf = ret[i]

				if jfilter:
					if 'stock' not in abuf:
						raise ValueError("A StockDocument is required to filter journal entries")
					in_arr = abuf['stock']['journal'] # type: ignore
				else:
					in_arr = abuf[fb.data] # type: ignore

				fres = fb.filter.apply(in_arr)

				if debug:
					self.logger.debug(f"{fb.descr} result: in: {len(in_arr)}, out: {len(fres)}")

				if fb.include:
					if not fres: # matched elements should be include but no match
						ret.pop(i)
				elif fres: # elements were matched but on_match is exclude
					ret.pop(i)

		return ret
