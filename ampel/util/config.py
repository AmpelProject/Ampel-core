#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/config.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                06.04.2023
# Last Modified Date:  06.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.core.UnitLoader import UnitLoader
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.UnitModel import UnitModel
from ampel.util.hash import build_unsafe_dict_id


def get_unit_confid(loader: 'UnitLoader', unit: str, config: dict[str, Any]) -> int:
	return build_unsafe_dict_id(
		loader.new_logical_unit(  # noqa: SLF001
			model = UnitModel(unit=unit, config=config),
			logger = AmpelLogger.get_logger()
		)._get_trace_content(),
		ret = int
	)
