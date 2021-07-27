#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsEventUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.02.2020
# Last Modified Date: 14.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict, Optional, Sequence
from ampel.types import ChannelId
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit
from ampel.model.UnitModel import UnitModel
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
	process_name: str

	#: channels associated with the process
	channel: Optional[ChannelId] = None

	#: raise exceptions instead of catching and logging
	raise_exc: bool = False

	#: logging configuration to use, from the logging section of the Ampel config
	log_profile: str = "default"

	#: flag to apply to all log records created
	base_log_flag: LogFlag = LogFlag.SCHEDULED_RUN

	#: optional additional kwargs to pass to :class:`~ampel.mongo.update.var.DBLoggingHandler.DBLoggingHandler`
	db_handler_kwargs: Optional[Dict[str, Any]] = None

	#: Some subclasses allow for non-serializable input parameter out of convenience (jupyter notebooks).
	#: For example, an AlertSupplier instance can be passed as argument of an AlertConsumer.
	#: Unless a custom serialization is implemented, this disables the hashing of input parameters
	#: and thus the generation of "trace ids".
	#: Set provenance to False to ignore trace ids generation error which will be raised otherwise.
	provenance: bool = True

	#: Provides contextual informations to this run (fills session_info)
	session: Optional[Sequence[UnitModel]]


	@abstractmethod(var_args=True)
	def run(self) -> Any:
		...
