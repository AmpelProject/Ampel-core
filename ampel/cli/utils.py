#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/utils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.03.2021
# Last Modified Date: 24.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.util.collections import check_seq_inner_type


def maybe_convert_stock(args: Dict[str, Any]) -> None:
	"""
	Replaces the string id defined in args['id_mapper'] with an instance of the requested id mapper.
	Replaces potential string stock ids with their ampel ids.
	"""

	args['id_mapper'] = AuxUnitRegister.get_aux_class(
		args['id_mapper'], sub_type=AbsIdMapper
	)() if args['id_mapper'] else None

	if (
		args['id_mapper'] and args['id_mapper'] and
		(isinstance(args['stock'], str) or check_seq_inner_type(args['stock'], str))
	):
		args['stock'] = args['id_mapper'].to_ampel_id(args['stock']) # type: ignore
