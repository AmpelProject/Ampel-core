#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/distrib.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                13.03.2021
# Last Modified Date:  23.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, re
from collections.abc import Generator
from pkg_resources import ( # type: ignore[attr-defined]
	get_distribution, AvailableDistributions,
	EggInfoDistribution, DistInfoDistribution
)


def get_dist_names(distrib_prefixes: list[str] = ["ampel-", "pyampel-"]) -> list[str]:
	""" Get all installed distributions whose names start with the provided prefix """
	# ensure that at least interface and core are found
	prefixes = {"ampel-interface", "ampel-core"}.union(distrib_prefixes)
	ret = [
		dist_name for dist_name in AvailableDistributions()
		if any(prefix in dist_name for prefix in prefixes)
	]

	if ret:
		ret.insert(0, ret.pop([i for i, el in enumerate(ret) if "interface" in el][0]))
		ret.insert(0, ret.pop([i for i, el in enumerate(ret) if "core" in el][0]))

	return ret


def get_files(
	dist_name: str,
	lookup_dir: None | str = None,
	pattern: None | re.Pattern = None
) -> list[str]:
	""" Loads all known conf files of the provided distribution (name) """

	if lookup_dir and not lookup_dir.endswith("/"):
		lookup_dir += "/"

	distrib = get_distribution(dist_name)

	# DistInfoDistribution: look for metadata RECORD (in <dist_name>.dist-info)
	if isinstance(distrib, DistInfoDistribution):

		# "pip3 install" in editable mode with pyproject.toml present
		# 1) grab the pth file
		if pth := next(
			(
				pth for el in distrib.get_metadata_lines('RECORD')
				if (pth := el.split(",")[0]).endswith(".pth")
			),
			None
		):

			fname = pth if os.path.isfile(pth) else distrib.get_resource_filename(__name__, pth)
			# 2) Manually look for files in referenced folder
			with open(fname, "r") as f:
				return list(
					walk_dir(
						f.read().strip(),
						lookup_dir,
						pattern
					)
				)

		# <pip3 install .> in non-editable mode with pyproject.toml present
		# Example of the ouput of distrib.get_metadata_lines('RECORD'):
		# 'conf/ampel-ztf.conf,sha256=FZkChNKKpcMPTO4pwyKq4WS8FAbznuR7oL9rtNYS7U0,322',
		# 'ampel/model/ZTFLegacyChannelTemplate.py,sha256=zVtv4Iry3FloofSazIFc4h8l6hhV-wpIFbX3fOW2njA,2182',
		# 'ampel/model/__pycache__/ZTFLegacyChannelTemplate.cpython-38.pyc,,',
		else:
			return [
				fname for el in distrib.get_metadata_lines('RECORD')
				if _check_match((fname := el.split(",")[0]), lookup_dir, pattern)
			]

	# "pip3 install" in editable mode without pyproject.toml present
	elif isinstance(distrib, EggInfoDistribution):
		# Example of the ouput of distrib.get_metadata_lines('SOURCES.txt'):
		# 'setup.py',
		# 'conf/ampel-ztf.json',
		# 'ampel/model/ZTFLegacyChannelTemplate.py',
		return [
			el for el in distrib.get_metadata_lines('SOURCES.txt')
			if _check_match(el, lookup_dir, pattern)
		]

	else:
		raise ValueError(f"Unsupported distribution type: '{type(distrib)}'")


def _check_match(
	arg: str,
	lookup_dir: None | str,
	pattern: None | re.Pattern
) -> bool:
	if lookup_dir and not arg.startswith(lookup_dir):
		return False
	if pattern and not pattern.match(arg):
		return False
	return True


def walk_dir(
	path: str,
	lookup_dir: None | str = None,
	pattern: None | re.Pattern = None
) -> Generator[str, None, None]:
	for root, dirs, files in os.walk(f"{path}/{lookup_dir}" if lookup_dir else f"{path}"):
		for fname in files:
			if pattern and not pattern.match(fname):
				continue
			yield os.path.join(root, fname)
