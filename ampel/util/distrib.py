#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/distrib.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                13.03.2021
# Last Modified Date:  23.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, re, importlib, json
from typing import TypeAlias
from collections.abc import Generator, MutableSequence, Sequence
from importlib import metadata
from pathlib import Path

# NB: PackagePath implements read_text(), but is not a subclass of Path
PathLike: TypeAlias = Path | metadata.PackagePath
PathList: TypeAlias = MutableSequence[PathLike]


def get_dist_names(distrib_prefixes: Sequence[str] = ("ampel-", "pyampel-")) -> list[str]:
	""" Get all installed distributions whose names start with the provided prefix """

	# ensure that at least interface and core are found
	prefixes = {"ampel-interface", "ampel-core"}.union(distrib_prefixes)
	ret = [
		dist.name for dist in metadata.distributions()
		if any(dist.name.startswith(prefix) for prefix in prefixes)
	]

	ret = list(set(ret))  # remove duplicates, yes it can happen
	if ret:
		ret.insert(0, ret.pop(next(i for i, el in enumerate(ret) if "interface" in el)))
		ret.insert(0, ret.pop(next(i for i, el in enumerate(ret) if "core" in el)))

	return ret


def get_files(
	dist_name: str,
	lookup_dir: str | None = None,
	pattern: re.Pattern | None = None
) -> PathList:
	""" Loads all known conf files of the provided distribution (name) """

	if lookup_dir and not lookup_dir.endswith("/"):
		lookup_dir += "/"

	files: PathList = []

	for dist in metadata.distributions():
		if dist.name != dist_name:
			continue

		# Try direct_url.json first — even if no .pth file exists
		mod_path = resolve_direct_url_path(dist_name)
		if mod_path and os.path.isdir(mod_path):
			files.extend(walk_dir(mod_path, lookup_dir, pattern))
			break  # skip .pth parsing if direct path works

		for p in dist.files or []:
			if p.suffix == ".pth":
				pth_path = Path(str(dist.locate_file(p)))
				lines = pth_path.read_text().splitlines()
				for line in lines:
					line = line.strip() # noqa PLW2901
					if os.path.isdir(line):
						files.extend(walk_dir(line, lookup_dir, pattern))
					elif line.startswith("import __editable__"):
						mod_path = resolve_editable_hook_path(pth_path)
						if not mod_path:
							mod_path = resolve_direct_url_path(dist_name)
						if mod_path:
							files.extend(walk_dir(mod_path, lookup_dir, pattern))
						else:
							raise ValueError(
								f"Could not resolve module path from editable hook or direct_url.json in {pth_path}. "
								f"Try installing {dist_name} normally (non-editable)."
							)
					else:
						raise ValueError(
							f"Invalid path '{line}' in {pth_path}. "
							f"If {dist_name} was installed with setuptools, it must be installed normally (not in editable mode)."
						)
			elif _check_match(p.as_posix(), lookup_dir, pattern):
				files.append(Path(str(dist.locate_file(p))))

	return files


def _check_match(
	path: str,
	lookup_dir: str | None,
	pattern: re.Pattern | None
) -> bool:
	if lookup_dir and not path.startswith(lookup_dir):
		return False
	return not (pattern and not pattern.match(path))


def walk_dir(
	path: str,
	lookup_dir: str | None = None,
	pattern: re.Pattern | None = None
) -> Generator[Path, None, None]:

	for root, _, files in os.walk(path):
		for fname in files:
			full_path = os.path.join(root, fname)
			rel_path = os.path.relpath(full_path, path)
			if _check_match(rel_path, lookup_dir, pattern):
				yield Path(full_path)


def resolve_editable_hook_path(pth_path: Path) -> str | None:
	try:
		text = pth_path.read_text().strip()
		match = re.search(r'import\s+(__editable__[^;]+)', text)
		if match:
			finder_module = match.group(1)
			finder = importlib.import_module(finder_module)
			if finder.__file__ is not None:
				return os.path.dirname(finder.__file__)
	except Exception:
		pass
	return None


def resolve_direct_url_path(dist_name: str) -> str | None:
	try:
		dist = metadata.distribution(dist_name)
		direct_url = dist.read_text("direct_url.json")
		if direct_url:
			info = json.loads(direct_url)
			if info.get("dir_info", {}).get("editable") and info.get("url", "").startswith("file://"):
				return info["url"][7:]  # strip 'file://'
	except Exception:
		pass
	return None
