#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/ChainedIngestionHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2020
# Last Modified Date: 06.09.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Literal, Sequence, List, Dict, Union, Tuple, Optional, Any, Set, Callable
from ampel.types import StockId, ChannelId, UnitId, DataPointId, UBson
from ampel.abstract.AbsT0Muxer import AbsT0Muxer
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsApplicable import AbsApplicable
from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.abstract.AbsT1ComputeUnit import AbsT1ComputeUnit
from ampel.abstract.AbsT1CombineUnit import AbsT1CombineUnit
from ampel.abstract.AbsT1RetroCombineUnit import AbsT1RetroCombineUnit
from ampel.model.UnitModel import UnitModel
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.model.ingest.CompilerOptions import CompilerOptions
from ampel.model.ingest.T1Combine import T1Combine
from ampel.model.ingest.T1CombineCompute import T1CombineCompute
from ampel.model.ingest.T1CombineComputeNow import T1CombineComputeNow
from ampel.model.ingest.T2Compute import T2Compute
from ampel.model.ingest.IngestDirective import IngestDirective
from ampel.model.ingest.DualIngestDirective import DualIngestDirective
from ampel.model.ingest.IngestBody import IngestBody
from ampel.core.AmpelContext import AmpelContext
from ampel.base.LogicalUnit import LogicalUnit
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.content.StockDocument import StockDocument
from ampel.content.DataPoint import DataPoint
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.ingest.T0Compiler import T0Compiler
from ampel.ingest.T2Compiler import T2Compiler
from ampel.ingest.StockCompiler import StockCompiler
from ampel.ingest.T1Compiler import T1Compiler
from ampel.struct.UnitResult import UnitResult
from ampel.struct.T1CombineResult import T1CombineResult
from ampel.log import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.util.hash import build_unsafe_dict_id


class T2Block:
	__slots__ = "unit", "config", "slc", "sort", "filter", "group"
	unit: UnitId
	config: Optional[int]
	filter: Optional[AbsApplicable]
	sort: Optional[Callable]
	slc: Optional[slice]
	group: Optional[List[int]]


class T1ComputeBlock:
	__slots__ = 'unit', 'unit_name', 'config', 'trace_id'
	unit: Optional[AbsT1ComputeUnit]
	unit_name: Optional[UnitId]
	config: Optional[int]
	trace_id: int


class T1CombineBlock:
	__slots__ = 'unit', 'trace_id', 'compute', 'channel', 'group', 'state_t2', 'point_t2'
	unit: Union[AbsT1CombineUnit, AbsT1RetroCombineUnit]
	trace_id: int
	compute: T1ComputeBlock
	channel: ChannelId
	group: Optional[List[int]]
	state_t2: Optional[List[T2Block]]
	point_t2: Optional[List[T2Block]]


class T0MuxBlock:
	__slots__ = 'unit', 'config', 'trace_id', 'combine', 'point_t2'
	unit: AbsT0Muxer
	config: Optional[int]
	trace_id: int
	combine: Optional[List[T1CombineBlock]]  # mux.combine
	point_t2: Optional[List[T2Block]] # mux.insert.point_t2


class IngestBlock:
	__slots__ = 'channel', 'mux', 'combine', 'combine', 'point_t2', 'stock_t2'
	channel: ChannelId
	mux: Optional[T0MuxBlock]
	combine: Optional[List[T1CombineBlock]] # combine blocks
	point_t2: Optional[List[T2Block]] # point t2
	stock_t2: Optional[List[T2Block]] # stock t2


T1CombineCache = Dict[
	Tuple[Union[AbsT1CombineUnit, AbsT1RetroCombineUnit], Tuple[DataPointId, ...]],
	Tuple[List[Union[List[DataPointId], T1CombineResult]], Set[ChannelId]]
]

T1ComputeCache = Dict[
	Tuple[AbsT1ComputeUnit, Tuple[DataPointId, ...]],
	Union[Tuple[Union[UBson, UnitResult], StockId]]
]


class ChainedIngestionHandler:

	__slots__ = '__dict__', 'updates_buffer', 'ingest_stats', 'iblocks', 'shaper', \
		'shaper_trace_id', 't0_compiler', 't1_compiler', 'stock_compiler', \
		'state_t2_compiler', 'point_t2_compiler', 'stock_t2_compiler', \
		'stock_ingester', 't0_ingester', 't1_ingester', 't2_ingester'

	def __init__(self,
		context: AmpelContext,
		shaper: UnitModel,
		directives: Sequence[Union[IngestDirective, DualIngestDirective]],
		updates_buffer: DBUpdatesBuffer,
		run_id: int,
		trace_id: Dict[str, int],
		tier: Literal[-1, 0, 1, 2, 3],
		compiler_opts: CompilerOptions,
		logger: AmpelLogger,
		database: str = "mongo",
		origin: Optional[int] = None,
		int_time: bool = True
	):
		"""
		:param trace_id: base trace if of root caller (such as AlertConsumer or T1Generator)
		:param int_time: timestamp accuracy for journal/meta (otherwise float)
		"""

		self.updates_buffer = updates_buffer
		self.logger = logger
		self.context = context
		self.run_id = run_id
		self.int_time = int_time
		self.base_trace_id = trace_id

		self.iblocks: List[Tuple[IngestBlock, IngestBlock]] = []
		self.ingest_stats: List[float] = []
		self._mux_cache: Dict[int, AbsT0Muxer] = {}
		self._t1_combine_units_cache: Dict[int, Union[AbsT1CombineUnit, AbsT1RetroCombineUnit]] = {}
		self._t1_compute_units_cache: Dict[int, AbsT1ComputeUnit] = {}

		if not directives:
			raise ValueError("Need at least 1 directive")

		self.shaper = self.context.loader.new_logical_unit(
			model=shaper, logger=logger, sub_type=AbsT0Unit
		)
		self.shaper_trace_id = self.shaper._trace_id

		# Base compiler parameters
		bopts: Dict[str, Any] = {"origin": origin, "tier": tier, "run_id": run_id}

		# Create compilers
		self.t0_compiler = T0Compiler(**(compiler_opts.t0 | bopts))
		self.t1_compiler = T1Compiler(**(compiler_opts.t1 | bopts))
		self.stock_compiler = StockCompiler(**(compiler_opts.stock | bopts))
		self.state_t2_compiler = T2Compiler(**(compiler_opts.state_t2 | bopts))
		self.point_t2_compiler = T2Compiler(**(compiler_opts.point_t2 | bopts), col="t0")
		self.stock_t2_compiler = T2Compiler(**(compiler_opts.stock_t2 | bopts), col='stock')

		# Create ingesters
		dbconf = self.context.config.get(f'{database}.ingest', dict, raise_exc=True)
		self.t0_ingester = AuxUnitRegister.new_unit(
			model = UnitModel(unit=dbconf['t0']),
			sub_type = AbsDocIngester[DataPoint],
			updates_buffer = updates_buffer
		)

		self.t1_ingester = AuxUnitRegister.new_unit(
			model = UnitModel(unit=dbconf['t1']),
			sub_type = AbsDocIngester[T1Document],
			updates_buffer = updates_buffer
		)

		self.t2_ingester = AuxUnitRegister.new_unit(
			model = UnitModel(unit=dbconf['t2']),
			sub_type = AbsDocIngester[T2Document],
			updates_buffer = updates_buffer
		)

		self.stock_ingester = AuxUnitRegister.new_unit(
			model = UnitModel(unit=dbconf['stock']),
			sub_type = AbsDocIngester[StockDocument],
			updates_buffer = updates_buffer
		)

		for directive in directives:
			self.iblocks.append(
				self._new_ingest_blocks(directive, updates_buffer, logger)
			)

		"""
		if logger.verbose:
			logger.log(VERBOSE,
				f"Ingesters: datapoint: 1, stock: 1, "
				f"t1: {len(self.state_t2_ingesters)}, "
				f"t2_state: {sum(len(v) for v in self.state_t2_ingesters.values())}, "
				f"t2_point: {len(self.point_t2_ingesters)}, "
				f"t2_stock: {len(self.stock_t2_ingesters)}"
			)
		"""

	def _new_ingest_blocks(self,
		directive: Union[IngestDirective, DualIngestDirective],
		updates_buffer: DBUpdatesBuffer,
		logger: AmpelLogger
	) -> Tuple[IngestBlock, IngestBlock]:

		if isinstance(directive, DualIngestDirective):
			known_ib = self._new_ingest_block(
				directive.ingest['known'], directive.channel, updates_buffer, logger
			)
			new_ib = self._new_ingest_block(
				directive.ingest['new'], directive.channel, updates_buffer, logger
			)
			return known_ib, new_ib

		elif isinstance(directive, IngestDirective):
			ib = self._new_ingest_block(
				directive.ingest, directive.channel, updates_buffer, logger
			)
			return ib, ib

		else:
			ValueError("Unknown directive type")
			

	def _new_ingest_block(self,
		directive: IngestBody,
		channel: ChannelId,
		updates_buffer: DBUpdatesBuffer,
		logger: AmpelLogger
	) -> IngestBlock:

		ib = IngestBlock()
		ib.mux = None
		ib.combine = None
		ib.stock_t2 = None
		ib.point_t2 = None
		ib.channel = channel

		if directive.mux:

			muxb = ib.mux = T0MuxBlock()
			muxb.trace_id = 0
			muxb.combine = None
			muxb.point_t2 = None

			i = build_unsafe_dict_id(directive.mux.dict(exclude_unset=False))
			if i in self._mux_cache:
				muxer = self._mux_cache[i]
			else:

				# We create a logger associated a buffering handler
				# whose logs entries are later transfered to the main logger
				buf_hdlr = DefaultRecordBufferingHandler(level=self.logger.level)
				buf_logger = AmpelLogger.get_logger(
					base_flag = (getattr(self.logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
					console = False,
					handlers = [buf_hdlr]
				)

				# Spawn new instance
				self._mux_cache[i] = muxer = self.context.loader.new_context_unit(
					model = directive.mux, context = self.context, sub_type = AbsT0Muxer,
					logger = buf_logger, updates_buffer = updates_buffer
				)

				# Shortcut to avoid muxer.logger.handlers[?]
				setattr(muxer, '_buf_hdlr', buf_hdlr)

			muxb.unit = muxer
			muxb.trace_id = muxer._trace_id

			if directive.mux.combine:

				# States, and T2s based thereon
				muxb.combine = [
					self._setup_t1_combine(channel, t1_combine)
					for t1_combine in directive.mux.combine
				]

			if directive.mux.insert:
				ib.mux.point_t2 = [
					self._gen_t2_block(el)
					for el in directive.mux.insert['point_t2']
				]

		if directive.combine:

			# States, and T2s based thereon
			ib.combine = [
				self._setup_t1_combine(channel, t1_combine)
				for t1_combine in directive.combine
			]

		# (Data)Point T2s
		if directive.point_t2:
			ib.point_t2 = [self._gen_t2_block(el) for el in directive.point_t2]

		# Stock T2s
		if directive.stock_t2:
			ib.stock_t2 = [self._gen_t2_block(el) for el in directive.stock_t2]

		return ib


	def _setup_t1_combine(self,
		channel: ChannelId,
		t1_combine: Union[T1Combine, T1CombineCompute, T1CombineComputeNow]
	) -> T1CombineBlock:
		"""
		Add the ingesters specified in ``t1_combine`` to ``cache``, reusing
		existing instances if possible.
		
		:param t1_combine: subclause of ingestion directive
		:param channel: channel of parent directive
		"""

		t1b = T1CombineBlock()
		t1b.trace_id = 0
		t1b.channel = channel
		t1b.group = None
		t1b.state_t2 = None
		t1b.point_t2 = None

		# Avoid 'ifs' later on if initiliazed with Nones
		t1b.compute = T1ComputeBlock()
		t1b.compute.unit = None
		t1b.compute.unit_name = None
		t1b.compute.config = None

		# Cache t1 combine units
		i = build_unsafe_dict_id(t1_combine.dict(exclude_unset=False))
		if i in self._t1_combine_units_cache:
			t1_unit = self._t1_combine_units_cache[i]
		else:

			# We create a logger associated a buffering handler
			# whose logs entries are later transfered to the main logger
			buf_hdlr = DefaultRecordBufferingHandler(level=self.logger.level)
			buf_logger = AmpelLogger.get_logger(
				base_flag = (getattr(self.logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = False,
				handlers = [buf_hdlr]
			)

			chan_dict = self.context.config.get(f'channel.{channel}', dict, raise_exc=True)
			t1_unit = self.context.loader.new_logical_unit(
				model = t1_combine, # model.config can potentially include/define channel (for chan-specific exclusions)
				logger = buf_logger,
				sub_type = AbsT1CombineUnit,
				access = chan_dict['access'],
				policy = chan_dict['policy']
			)

			# Shortcut to avoid t1_unit.logger.handlers[?]
			setattr(t1_unit, '_buf_hdlr', buf_hdlr)
			self._t1_combine_units_cache[t1b.trace_id] = t1_unit

		t1b.unit = t1_unit
		t1b.trace_id = t1_unit._trace_id
		if g := t1_combine.group:
			t1b.group = g if isinstance(g, list) else [g]

		# State T2s (are defined along with t1 directives usually)
		# We allow the definition of multiple combiners t2 ingesters
		if isinstance(t1_combine, (T1Combine, T1CombineComputeNow)):
			if t1_combine.state_t2:
				t1b.state_t2 = [self._gen_t2_block(el) for el in t1_combine.state_t2]
			if t1_combine.point_t2:
				t1b.point_t2 = [self._gen_t2_block(el) for el in t1_combine.point_t2]

		if isinstance(t1_combine, (T1CombineCompute, T1CombineComputeNow)):

			t1b.compute = T1ComputeBlock()
			t1b.compute.unit_name = t1_combine.compute.unit
			if isinstance(t1_combine.compute.config, int):
				t1b.compute.config = t1_combine.compute.config
			else:
				raise ValueError("Integer expected for t1_combine.compute.config")

			# On the fly t1 computation requested
			if isinstance(t1_combine, T1CombineComputeNow):

				# Cache t1 compute units
				i = build_unsafe_dict_id(t1_combine.compute.dict(exclude_unset=False))
				if i in self._t1_compute_units_cache:
					t1_compute_unit = self._t1_compute_units_cache[i]
				else:
					t1_compute_unit = self.context.loader.new_logical_unit(
						model = t1_combine,
						logger = buf_logger,
						sub_type = AbsT1ComputeUnit
					)
					self._t1_compute_units_cache[i] = t1_compute_unit

				t1b.compute.unit = t1_compute_unit
				t1b.compute.trace_id = t1_compute_unit._trace_id

		return t1b


	def _gen_t2_block(self, im: T2Compute) -> T2Block:

		ingest_opts: Dict[str, Any] = {}
		if not (t2_info := self.context.config.get(f'unit.{im.unit}', dict)):
			raise ValueError(f'Unknown T2 unit {im.unit}')

		# The unit is installed locally
		if 'fqn' in t2_info:
			ingest_opts = getattr(
				self.context.loader.get_class_by_name(name=im.unit, unit_type=LogicalUnit),
				'eligible', {}
			)

		# TODO: check comment (examples might be outdated)
		# Ingest options prevalence/priority:
		# 1) Static class member 'eligible' (ex: T2CatMatch might define: {'eligible': {'select': 'first'}})
		# 2) Specific ingest config might define: {'ingest': {'select': 'all'}}
		if im.ingest:
			ingest_opts.update(im.ingest)

		ib = T2Block()
		ib.unit = im.unit
		ib.config = im.config # type: ignore[assignment]

		# Save confid to external collection for posterity
		if isinstance(im.config, int):
			# Make sure config is valid and do provenance check
			self.context.loader.get_init_config(im.config)

		# Only for point t2 units (which can customize the ingestion)
		if ingest_opts:

			ib.filter = AuxUnitRegister.new_unit(
				UnitModel(unit=ingest_opts['filter']),
				sub_type=AbsApplicable
			) if 'filter' in ingest_opts else None

			if 'sort' in ingest_opts:
				if ingest_opts['sort'] == 'id':
					f = lambda k: k['id']
				else:
					f = lambda k: k['body'][ingest_opts['sort']]
				ib.sort = lambda l: sorted(l, key=f)
			else:
				ib.sort = None

			ib.slc = self._get_slice(ingest_opts['select']) \
				if 'select' in ingest_opts else None
		else:
			ib.filter = None
			ib.sort = None
			ib.slc = None

		if im.group:
			ib.group = [im.group] if isinstance(im.group, int) else im.group
		else:
			ib.group = None

		return ib


	def _get_slice(self,
		arg: Optional[Union[Literal['first'], Literal['last'], Tuple[int, int, int]]]
	) -> slice:
		"""
		:raises: ValueError if parameter 'arg' is invalid
		"""
		if arg is None:
			return slice(None)
		elif arg == "all":
			return slice(None)
		elif arg == "first":
			return slice(1)
		elif arg == "last":
			return slice(-1, -2, -1)
		elif isinstance(arg, list) and len(arg) == 3:
			return slice(*arg)
		else:
			raise ValueError(
				f"Unsupported value provided as slice parameter : {arg}"
			)


	def ingest(self,
		dps: List[DataPoint],
		filter_results: List[Tuple[int, Union[bool, int]]],
		stock_id = 0,
		extra: Optional[Dict[str, Any]] = None,
	) -> None:
		"""
		Create database documents.
		:param filter_results: the value returned from
		  :func:`~ampel.abstract.AbsAlertFilter.AbsAlertFilter.process` if alert was accepted
		"""

		self.updates_buffer._block_autopush = True
		ingest_start = time()
		now = int(time()) if self.int_time else time()

		# process *modifies* dict instances loaded by fastavro
		dps = self.shaper.process(dps, stock_id)

		if not dps: # Not sure if this can happen
			return

		# Set of chans (last parameter) is used for logging
		mux_cache: Dict[AbsT0Muxer, Tuple[Optional[List[DataPoint]], Optional[List[DataPoint]], Set[ChannelId]]] = {}
		t1_comb_cache: T1CombineCache = {}
		t1_comp_cache: T1ComputeCache = {}

		# ingestion blocks
		ibs = self.iblocks

		for i, fres in filter_results:

			# Add alert and shaper version info to stock journal entry
			jentry: Dict[str, Any] = {
				'action': JournalActionCode.STOCK_ADD_CHANNEL,
				'traceid': self.base_trace_id | {'shaper': self.shaper_trace_id}
			}

			if extra:
				jentry = extra | jentry

			if i > 0: # Known stock (for the current channel)
				ib = ibs[i][0]
			else: # New stock
				ib = ibs[-i][1]

			# Muxer requested
			if mux := ib.mux:

				# Add muxer version info to stock journal entry
				jentry['traceid']['muxer'] = mux.trace_id

				# Potentially load previous results from cache
				if mux.unit in mux_cache:
					dps_insert, dps_combine, s = mux_cache[mux.unit]
					s.add(ib.channel)
				else:
					dps_insert, dps_combine = mux.unit.process(dps, stock_id)
					mux_cache[mux.unit] = dps_insert, dps_combine, {ib.channel}

				if dps_combine:

					if x := [
						dp for dp in dps_combine
						if 'channel' in dp and ib.channel not in dp['channel']
					]:
						dps_insert = (dps_insert + x) if dps_insert else x

				if dps_insert:

					self.t0_compiler.add(dps_insert, ib.channel, self.shaper_trace_id)

					# TODO: make this addition optional (a stock with a million dps would create pblms)
					jentry['upsert'] = [el['id'] for el in dps_insert]
					jentry['action'] |= JournalActionCode.T0_ADD_CHANNEL

					if mux.point_t2:
						jentry['action'] |= JournalActionCode.T2_ADD_CHANNEL
						self.ingest_point_t2s(dps_insert, fres, stock_id, ib.channel, mux.point_t2)

				# Muxed T1 and associated T2 ingestions
				if dps_combine and mux.combine:
					self.ingest_t12(dps_combine, fres, stock_id, jentry, mux.combine, t1_comb_cache, t1_comp_cache)

			else:
				self.t0_compiler.add(dps, ib.channel, self.shaper_trace_id)

			# Non-muxed T1 and associated T2 ingestions
			if ib.combine:
				self.ingest_t12(dps, fres, stock_id, jentry, ib.combine, t1_comb_cache, t1_comp_cache)
				
			# Non-muxed point T2s
			if ib.point_t2:
				self.ingest_point_t2s(dps, fres, stock_id, ib.channel, ib.point_t2)

			# Stock T2s
			if ib.stock_t2:
				for t2b in ib.stock_t2:
					self.stock_t2_compiler.add(
						t2b.unit, t2b.config, stock_id, stock_id, ib.channel, {'traceid': self.base_trace_id}
					)

			# Flush potential unit logs
			###########################

			logger = self.logger
			for muxer, (_, _, chans) in mux_cache.items():
				if muxer._buf_hdlr.buffer: # type: ignore[attr-defined]
					muxer._buf_hdlr.forward( # type: ignore[attr-defined]
						logger, stock=stock_id, channel=list(chans), extra = extra
					)

			for (t1_unit, _), (_, chans) in t1_comb_cache.items():
				if t1_unit._buf_hdlr.buffer: # type: ignore[union-attr]
					t1_unit._buf_hdlr.forward( # type: ignore[union-attr]
						logger, stock=stock_id, channel=list(chans), extra = extra
					)

			if not self.stock_compiler.register:
				self.stock_compiler.add(stock_id, ib.channel, journal=jentry) # type: ignore[arg-type]

		# Commit
		########

		self.t0_compiler.commit(self.t0_ingester, now)

		if self.t1_compiler.t1s:
			self.t1_compiler.commit(self.t1_ingester, now)

		if self.state_t2_compiler.t2s:
			self.state_t2_compiler.commit(self.t2_ingester, now)

		if self.stock_t2_compiler.t2s:
			self.stock_t2_compiler.commit(self.t2_ingester, now)

		if self.point_t2_compiler.t2s:
			self.point_t2_compiler.commit(self.t2_ingester, now)

		self.stock_compiler.commit(self.stock_ingester, now)
		self.ingest_stats.append(time() - ingest_start)
		self.updates_buffer._block_autopush = False


	def ingest_point_t2s(self,
		dps: List[DataPoint], fres: Union[bool, int],
		stock_id: StockId, channel: ChannelId, state_t2: List[T2Block]
	) -> None:

		for t2b in state_t2:

			# Filter group selection / veto
			if t2b.group and isinstance(fres, int) and fres not in t2b.group:
				continue

			# filter (ex: use only photopoints or upperlimis)
			f = t2b.filter.apply(dps) if t2b.filter else dps

			# Sort (ex: by body.jd)
			if t2b.sort:
				f = t2b.sort(f)

			# Slice (ex: first datapoint)
			if t2b.slc:
				f = f[t2b.slc]

			if isinstance(f, list):
				for el in f:
					self.point_t2_compiler.add(
						t2b.unit, t2b.config, stock_id, el['id'], channel, {'traceid': self.base_trace_id}
					)
			else:
				self.point_t2_compiler.add(
					t2b.unit, t2b.config, stock_id, f['id'], channel, {'traceid': self.base_trace_id}
				)


	def ingest_t12(self,
		dps: List[DataPoint], fres: Union[bool, int], stock_id: StockId,
		jentry: Dict[str, Any], t1bs: List[T1CombineBlock],
		t1_comb_cache: T1CombineCache, t1_comp_cache: T1ComputeCache
	) -> None:

		tdps = tuple(el['id'] for el in dps)

		# Loop through t1 blocks
		for t1b in t1bs:

			# Skip unmatched group
			if t1b.group and isinstance(fres, int) and fres not in t1b.group:
				continue

			# Potentially load previous results from cache
			if (t1b.unit, tdps) in t1_comb_cache:
				lres, s = t1_comb_cache[(t1b.unit, tdps)]
				s.add(t1b.channel)
			else:
				comb_res = t1b.unit.combine(iter(dps))
				if isinstance(comb_res, T1CombineResult): # case T1CombineResult
					lres = [comb_res]
				elif isinstance(comb_res, list):
					if len(comb_res) == 0:
						lres = []
					elif isinstance(comb_res[0], DataPointId): # case List[DataPointId]
						lres = [comb_res] # type: ignore[list-item]
					else:
						# case List[List[DataPointId]], List[T1CombineResult]
						lres = comb_res # type: ignore[assignment]
				t1_comb_cache[(t1b.unit, tdps)] = lres, {t1b.channel}

			# T1 combine(...) can return multiple subsets of the initial datapoints
			for tres in lres:

				body = None
				meta: Dict[str, Any] = {
					'action': MetaActionCode.ADD_CHANNEL,
					'traceid': self.base_trace_id | {'combiner': t1b.trace_id}
				}

				if 'muxer' in jentry['traceid']:
					meta['traceid']['muxer'] = jentry['traceid']['muxer']

				if 'alert' in jentry:
					meta['alert'] = jentry['alert']

				if isinstance(tres, T1CombineResult):
					t1_dps = tres.dps
					if tres.meta:
						meta |= tres.meta
						meta['action'] |= MetaActionCode.EXTRA_META
						jentry['action'] |= MetaActionCode.T1_EXTRA_META
					if tres.code:
						code = tres.code
						meta['action'] |= MetaActionCode.SET_CODE
						jentry['action'] |= MetaActionCode.T1_SET_CODE
					else:
						code = DocumentCode.OK
				else:
					t1_dps = tres
					code = DocumentCode.OK

				if excl := [el['id'] for el in dps if el['id'] not in t1_dps]:
					meta['excl'] = excl

				je = jentry.copy()
				je['traceid'] = jentry['traceid'].copy()
				je['traceid']['combiner'] = t1b.trace_id

				if not t1_dps:
					self.logger.info(f"No datapoints returned by t1 unit ({t1b.channel})")
					if stock_id:
						je['combine_empty'] = True
						self.stock_compiler.add(stock_id, t1b.channel, journal=je) # type: ignore[arg-type]
					continue

				# On the fly t1 computation requested
				if t1b.compute.unit:

					je['traceid']['t1_compute'] = t1b.compute.trace_id
					meta['action'] |= MetaActionCode.ADD_BODY

					# Potentially load previous results from "t1 compute" cache
					k = t1b.compute.unit, tuple(t1_dps)
					if k in t1_comp_cache:
						t1_res = t1_comp_cache[k]
					else:
						t1_res = t1_comp_cache[k] = t1b.compute.unit.compute(
							[dp for dp in dps if dp['id'] in t1_dps]
						)

					# AbsT1ComputeUnit can be used to determine stock
					stock_id = t1_res[1]

					if isinstance(t1_res[0], UnitResult):
						body = t1_res[0].body
						if t1_res[0].journal:
							je |= t1_res[0].journal.dict()
							meta['action'] |= MetaActionCode.EXTRA_JOURNAL
							je['action'] |= MetaActionCode.T1_EXTRA_JOURNAL
						if t1_res[0].code:
							code = t1_res[0].code
							meta['action'] |= MetaActionCode.SET_CODE
							je['action'] |= MetaActionCode.T1_SET_CODE
						else:
							code = DocumentCode.NEW
					else:
						body = t1_res[0]
						code = DocumentCode.NEW


				je['action'] |= JournalActionCode.T1_ADD_CHANNEL

				# Note: we ignore potential stock from T1 result here
				link = self.t1_compiler.add(
					t1_dps, t1b.channel, stock_id,
					meta = meta,
					unit = t1b.compute.unit_name,
					config = t1b.compute.config,
					body = body,
					code = code
				)

				je['link'] = link

				self.stock_compiler.add(stock_id, t1b.channel, journal=je) # type: ignore[arg-type]

				if t1b.state_t2:

					jentry['action'] |= JournalActionCode.T2_ADD_CHANNEL
					for t2b in t1b.state_t2:

						# Skip unmatched group
						if t2b.group and isinstance(fres, int) and fres not in t2b.group:
							continue

						self.state_t2_compiler.add(
							t2b.unit, t2b.config, stock_id, link, t1b.channel, {'traceid': meta['traceid']}
						)

				if t1b.point_t2:
				
					jentry['action'] |= JournalActionCode.T2_ADD_CHANNEL
					self.ingest_point_t2s(
						[el for el in dps if el['id'] in t1_dps],
						fres, stock_id, t1b.channel, t1b.point_t2
					)
