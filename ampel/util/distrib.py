#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/distrib.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                13.03.2021
# Last Modified Date:  23.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os
import re
from collections.abc import Generator, MutableSequence, Sequence
from importlib import metadata
from pathlib import Path
from typing import TypeAlias

# NB: PackagePath implements read_text(), but is not a subclass of Path
PathLike: TypeAlias = Path | metadata.PackagePath
PathList: TypeAlias = MutableSequence[PathLike]

def get_dist_names(distrib_prefixes: Sequence[str] = ("ampel-", "pyampel-")) -> list[str]:
	""" Get all installed distributions whose names start with the provided prefix """
	# ensure that at least interface and core are found
	prefixes = {"ampel-interface", "ampel-core"}.union(distrib_prefixes)
	ret = [
		dist.name for dist in metadata.distributions()
		if any(prefix in dist.name for prefix in prefixes)
	]

	if ret:
		ret.insert(0, ret.pop(next(i for i, el in enumerate(ret) if "interface" in el)))
		ret.insert(0, ret.pop(next(i for i, el in enumerate(ret) if "core" in el)))

	return ret


def get_files(
	dist_name: str,
	lookup_dir: None | str = None,
	pattern: None | re.Pattern = None
) -> PathList:
	""" Loads all known conf files of the provided distribution (name) """

	if lookup_dir and not lookup_dir.endswith("/"):
		lookup_dir += "/"

	files: PathList = []

	for p in metadata.files(dist_name) or []:
		# Path config file from editable install: look for files in the
		# referenced path(s)
		if p.suffix == ".pth":
			for path in p.read_text().splitlines():
				if os.path.isdir(path):
					files.extend(walk_dir(
						path,
						lookup_dir,
						pattern
					))
				else:
					# setuptools editable installs work by installing a path
					# hook. There's no way to get at the actual path to the
					# project without depending deeply on assumptions about how
					# setuptools currently works, so just bail.
					raise ValueError(
						f"Invalid path '{path}' in {p}. If {dist_name} was "
						"installed with setuptools, it must be installed "
						"normally (not in editable mode)."
					)
		# Wheel installs have the files directly in the distribution
		elif _check_match(p.as_posix(), lookup_dir, pattern):
			files.append(p)
	return files


def _check_match(
	arg: str,
	lookup_dir: None | str,
	pattern: None | re.Pattern
) -> bool:
	if lookup_dir and not arg.startswith(lookup_dir):
		return False
	return not (pattern and not pattern.match(arg))


def walk_dir(
	path: str,
	lookup_dir: None | str = None,
	pattern: None | re.Pattern = None
) -> Generator[Path, None, None]:
	for root, _, files in os.walk(f"{path}/{lookup_dir}" if lookup_dir else f"{path}"):
		for fname in files:
			if pattern and not pattern.match(fname):
				continue
			yield Path(root, fname)
