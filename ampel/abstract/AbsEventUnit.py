#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsEventUnit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                04.02.2020
# Last Modified Date:  25.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.types import ChannelId, OneOrMany, Traceless
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod, defaultmethod
from ampel.core.EventHandler import EventHandler
from ampel.core.ContextUnit import ContextUnit
from ampel.enum.EventCode import EventCode
from ampel.log.LogFlag import LogFlag


class AbsEventUnit(AmpelABC, ContextUnit, abstract=True):
	"""
	Base class for units orchestrating an ampel event.
	Ampel processes (from the ampel config) are the most common events.
	They define specifications of an event using a standardized model/schema:
	which subclass of this abstract class is to be run by a given controller
	at (a) given time(s) using a specific configuration.

	An ampel event must be registered and traceable and thus generates:
	- a new run-id
	- a new event document (in the event collection)
	- possibly Log entries (less is better)
	- new trace ids if not registered previously

	Subclassses of this class typically modify tier documents
	whereby the changes originate from results of logical unit(s)
	instantiated by this class.
	"""

	#: name of the associated process
	process_name: Traceless[str]

	#: hash of potentially underlying job schema
	job_sig: None | int = None

	#: channels associated with the process
	channel: None | OneOrMany[ChannelId] = None

	#: raise exceptions instead of catching and logging
	#: if True, troubles collection will not be populated if an exception occurs
	raise_exc: bool = False

	#: logging configuration to use, from the logging section of the Ampel config
	log_profile: str = "default"

	#: flag to apply to all log records created
	base_log_flag: LogFlag = LogFlag.SCHEDULED_RUN

	#: optional additional kwargs to pass to :class:`~ampel.mongo.update.var.DBLoggingHandler.DBLoggingHandler`
	db_handler_kwargs: None | dict[str, Any] = None

	#: Some subclasses allow for non-serializable input parameter out of convenience (jupyter notebooks).
	#: For example, an AlertSupplier instance can be passed as argument of an AlertConsumer.
	#: Unless a custom serialization is implemented, this disables the hashing of input parameters
	#: and thus the generation of "trace ids".
	#: Set provenance to False to ignore trace ids generation error which will be raised otherwise.
	provenance: bool = True


	@defaultmethod
	def prepare(self, event_hdlr: EventHandler) -> None | EventCode:
		"""
		Gives units the opportunity to cancel the subsequent call to the proceed() method
		by returning EventCode.PRE_CHECK_EXIT (which avoids 'burning' a run id)
		"""
		return None


	@abstractmethod(var_args=True)
	def proceed(self, event_hdlr: EventHandler) -> Any:
		...


	def run(self, event_hdlr: None | EventHandler = None) -> Any:

		if event_hdlr is None:
			event_hdlr = EventHandler(
				self.process_name,
				self.context.get_database(),
				job_sig = self.job_sig,
				raise_exc = self.raise_exc
			)

		# Give units opportunity to cancel the run (T2 worker when no tickets are avail for example)
		pre_check = self.prepare(event_hdlr)
		if pre_check == EventCode.PRE_CHECK_EXIT:
			event_hdlr.set_code(pre_check)
			event_hdlr.register()
			return None

		event_hdlr.register(
			run_id = self.context.new_run_id()
		)

		ret = None
		try:
			if ret := self.proceed(event_hdlr):
				event_hdlr.add_extra(ret=ret)
		except Exception as e:
			if self.raise_exc:
				raise e
			event_hdlr.code == EventCode.EXCEPTION

		# Set default event code if sub-class didn't customize it
		if event_hdlr.code is None:
			event_hdlr.code = EventCode.OK

		# Update duration and code in event doc
		event_hdlr.update()

		# Forward returned run() value to caller
		return ret
