#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/utils.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                24.03.2021
# Last Modified Date:  27.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os
from enum import IntEnum
from typing import Any, Literal, Type, overload
from ampel.core.AmpelDB import AmpelDB
from ampel.secret.AmpelVault import AmpelVault
from ampel.config.AmpelConfig import AmpelConfig
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.util.collections import check_seq_inner_type


def get_vault(args: dict[str, Any]) -> None | AmpelVault:
	vault = None
	if args.get('secrets'):
		from ampel.secret.DictSecretProvider import DictSecretProvider
		from ampel.secret.DirSecretProvider import DirSecretProvider
		if os.path.isdir(args['secrets']):
			provider: AbsSecretProvider = DirSecretProvider(args['secrets'])
		else:
			provider = DictSecretProvider.load(args['secrets'])
		vault = AmpelVault([provider])
	return vault


def get_db(
	config: AmpelConfig,
	vault: None | AmpelVault = None,
	require_existing_db: bool | str = True,
	one_db: bool | Literal['auto'] = False,
) -> AmpelDB:

	try:
		return AmpelDB.new(
			config,
			vault,
			require_exists = require_existing_db,
			one_db = one_db
		)
	except Exception as e:
		if "Databases with prefix" in str(e):
			s = "Databases with prefix " + (
				require_existing_db if isinstance(require_existing_db, str) else
				config.get('mongo.prefix', str, raise_exc=True)
			) + " do not exist"
			raise SystemExit("\n" + "="*len(s) + "\n" + s + "\n" + "="*len(s) + "\n")
		raise e


def _maybe_int(stringy):
	try:
		return int(stringy)
	except Exception:
		return stringy


def maybe_load_idmapper(args: dict[str, Any]) -> None:
	"""
	Replaces the string id defined in args['id_mapper'] with an instance of the requested id mapper.
	Replaces potential string stock ids with their ampel ids.
	"""

	args['id_mapper'] = AuxUnitRegister.get_aux_class(
		args['id_mapper'], sub_type=AbsIdMapper
	)() if args.get('id_mapper') else None

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


@overload
def maybe_resolve_enum(value: int | str, enum: Type[IntEnum]) -> int:
	...


@overload
def maybe_resolve_enum(value: list[int|str], enum: Type[IntEnum]) -> list[int]:
	...


def maybe_resolve_enum(value: int | str | list[int|str], enum: Type[IntEnum]) -> int | list[int]:
	"""
	Replace enum member names with their values
	"""
	if isinstance(value, str):
		if value in enum.__members__:
			return enum.__members__[value]
		else:
			raise ValueError(f"{value} is not a valid {enum.__name__}")
	elif isinstance(value, list):
		return [maybe_resolve_enum(v, enum) for v in value]
	elif isinstance(value, int):
		return value