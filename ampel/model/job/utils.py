#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/utils.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

import re
from typing import Union, Any, Callable
from ampel.model.job.ExpandWithItems import ExpandWithItems
from ampel.model.job.ExpandWithSequence import ExpandWithSequence
from ampel.util.recursion import walk_and_process_dict

ExpandWith = Union[None, ExpandWithItems, ExpandWithSequence]

def _parse_multiplier(values: dict[str, Any]) -> dict:

	if not isinstance(multiplier := values.pop("multiplier", 1), int):
		raise TypeError("multiplier must be an int")

	if multiplier > 1:
		assert (
			"expand_with" not in values
		), "multiplier and expand_with may not be used together"
		values |= {"expand_with": {"sequence": {"count": multiplier}}}

	return values

def _transform_item(v: str, transform: Callable[[str], str]) -> str:
	chunks = []
	pos = 0
	for match in re.finditer(r"\{\{(.*?)\}\}", v):
		if match.span()[0] > pos:
			chunks.append(v[pos: match.span()[0]])
		chunks.append(transform(match.groups()[0].strip()))
		pos = match.span()[1]
	if pos < len(v):
		chunks.append(v[pos: len(v)])
	return "".join(chunks)


def _transform_expressions_callback(path, k, d, **kwargs) -> None:
	for k, v in d.items():
		if isinstance(v, str):
			d[k] = _transform_item(v, kwargs["transform"])
		elif isinstance(v, list):
			d[k] = [
				_transform_item(vv, kwargs["transform"])
				if isinstance(vv, str)
				else vv
				for vv in v
			]


def transform_expressions(
	task_dict: dict, transformation: Callable[[str], str]
) -> dict:
	"""
	Replace any expressions of the form {{ expr }} with the result of
	transformation(expr)
	"""
	walk_and_process_dict(
		task_dict,
		callback = _transform_expressions_callback,
		transform = transformation
	)
	return task_dict
