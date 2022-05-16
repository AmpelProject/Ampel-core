#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/utils.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                24.03.2021
# Last Modified Date:  14.05.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.util.collections import check_seq_inner_type


def maybe_load_idmapper(args: dict[str, Any]) -> None:
	"""
	Replaces the string id defined in args['id_mapper'] with an instance of the requested id mapper.
	Replaces potential string stock ids with their ampel ids.
	"""

	args['id_mapper'] = AuxUnitRegister.get_aux_class(
		args['id_mapper'], sub_type=AbsIdMapper
	)() if args['id_mapper'] else None

	if not args['id_mapper']:
		return

	if isinstance(args['stock'], str):
		if "," in args['stock']:
			s = [int(x) if (x := el.strip()).isdigit() else x for el in args['stock'].split(",")]
			args['stock'] = args['id_mapper'].to_ampel_id(s)
		else:
			args['stock'] = args['id_mapper'].to_ampel_id(args['stock'])

	elif check_seq_inner_type(args['stock'], str):
		args['stock'] = args['id_mapper'].to_ampel_id(args['stock']) # type: ignore
