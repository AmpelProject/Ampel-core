#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/distrib.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                13.03.2021
# Last Modified Date:  06.11.2025
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, re, importlib, json
from typing import TypeAlias
from collections.abc import Generator, MutableSequence, Sequence, Iterable
from importlib import metadata
from pathlib import Path

# NB: PackagePath implements read_text(), but is not a subclass of Path
PathLike: TypeAlias = Path | metadata.PackagePath
PathList: TypeAlias = MutableSequence[PathLike]


def get_dist_names(distrib_prefixes: Sequence[str] = ("ampel-", "pyampel-")) -> list[str]:
	""" Get all installed distributions whose names start with the provided prefix """

	# ensure that at least interface and core are found
	prefixes = {"ampel-interface", "ampel-core"}.union(distrib_prefixes)
	ret = list({ # Note: list(set) because editable installs are found twice in distributions()
		dist.name for dist in metadata.distributions()
		if any(dist.name.startswith(prefix) for prefix in prefixes)
	})

	if ret:
		ret.insert(0, ret.pop(next(i for i, el in enumerate(ret) if "core" in el)))
		ret.insert(0, ret.pop(next(i for i, el in enumerate(ret) if "interface" in el)))

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


def get_classes_info_from_dist(
	pkg_name: str,
	sub_dir: str = "ampel",
	parent_map: dict[str, list[str]] | None = None
) -> dict[str, list[str]]:
	"""
	Returns a mapping from class file paths to their direct parent class names.

	This function scans a subdirectory within the specified installed package and identifies
	class definitions that follow the Ampel convention: each file defines a single class whose
	name matches the filename (excluding the `.py` extension). For each matching class,
	it extracts its direct parent class names and stores them in a dictionary.

	The result is a mapping where:
	  - keys are file paths to class definitions
	  - values are lists of direct parent class names, with generic type annotations removed

	:param pkg_name: Name of the installed package to inspect (e.g., 'ampel-interface').
	:param sub_dir: Subdirectory within the package to scan for class files.
	:param parent_map: Optional dictionary to populate with results. If provided, it will be updated.

	:return: A dictionary mapping each class file path to a list of its direct parent class names.

	:example:
		>>> get_classes_info_from_dist("ampel-interface")
		{
			'/Path/Ampel-interface/ampel/abstract/AbsT0Unit.py': ['LogicalUnit', 'abstract=True'],
			'/Path/Ampel-interface/ampel/abstract/AbsIdMapper.py': ['AmpelABC', 'AmpelBaseModel', 'abstract=True'],
			...
		}
	"""

	if parent_map is None:
		parent_map = {}

	for f in get_files(pkg_name, sub_dir, re.compile(r'.*/[A-Z][^/]*\.py$')):

		try:
			content = f.read_text()
		except Exception:
			continue  # skip unreadable files

		if match := re.search(rf'class\s+{f.stem}\((.*?)\):', content):
			raw = match.group(1)
			pattern = r'\[.*?\]' # handle AbsT3Supplier[Generator[AmpelBuffer, None, None]]
			while re.search(pattern, raw):
				raw = re.sub(pattern, '', raw)
			parents = [p.strip() for p in raw.replace(']', '').replace('[', '').split(',')]
			parent_map[str(f)] = parents

	return parent_map


def resolve_class_ancestry(
	file_path: str,
	parent_map: dict[str, list[str]],
	visited: set[str] | None = None
) -> list[str]:
	"""
	Recursively resolves the full inheritance chain for a class identified by its file path.

	This function traverses the `parent_map` to collect all direct and indirect parent class names
	for the class defined in the given file. It follows the Ampel convention: each file defines a
	single class whose name matches the filename (excluding the `.py` extension).

	:param file_path: Path to the class file, used as a key in `parent_map`.
	:param parent_map: Mapping of file paths to lists of direct parent class names.
	:param visited: Internal set used to track visited file paths and avoid cycles.

	:return: A flattened list of all inherited class names (direct and indirect).

	:example:
	>>> get_class_ancestry('/Path/Ampel-HU-cosmo/ampel/contrib/hu/t3/T3CosmicDipole.py', parent_map)
	['AbsT3Unit', 'LogicalUnit', 'AmpelABC', 'AmpelUnit', 'Generic[T]', 'SNeCosmicFlow']
	"""

	if visited is None:
		visited = set()

	if file_path in visited:
		return []

	visited.add(file_path)

	direct_parents = parent_map.get(file_path, [])
	resolved = []

	for parent in direct_parents:
		resolved.append(parent)
		# Try to find a file path in parent_map whose stem matches the parent class name
		for candidate_path in parent_map:
			if Path(candidate_path).stem == parent:
				resolved.extend(resolve_class_ancestry(candidate_path, parent_map, visited))
				break  # stop after first match

	return resolved


def get_classes_ancestry(
	parent_map: dict[str, list[str]],
	ancestry_map: dict[str, list[str]] | None = None,
	required_parents: Iterable[str] | None = None,
	exclude_parents: Iterable[str] | None = None,
	exclude_abstract: bool = True
) -> dict[str, list[str]]:
	"""
	Resolves the full inheritance chain for all classes defined in a parent map.

	This function iterates over each entry in `parent_map` and uses `resolve_class_ancestry`
	to collect all direct and indirect parent class names. It assumes the Ampel convention:
	each file defines a single class whose name matches the filename (excluding the `.py` extension).

	Filtering options:
	- If `required_parents` is provided, only classes whose ancestry includes at least one of the
	  specified parent names will be included.
	- If `exclude_parents` is provided, any class whose ancestry includes one of the excluded
	  parent names or whose name matches an excluded parent will be skipped.
	- If `exclude_abstract` is True, any class whose ancestry includes the marker "abstract=true"
	  will be excluded.

	:param parent_map: Dictionary mapping file paths to lists of direct parent class names.
	:param ancestry_map: Optional dictionary to populate with resolved inheritance chains.
	If not provided, a new one is created.
	:param required_parents: Optional list of parent class names to filter the output.
	:param exclude_parents: Optional list of parent class names to exclude from the output.
	:param exclude_abstract: If True, excludes classes marked as abstract.

	:return: A dictionary mapping each class file path to its full list of inherited class names.
	"""

	if ancestry_map is None:
		ancestry_map = {}

	# PLC0206: unpacking items() every loop less efficient than single key-based value access
	for fname in parent_map: # noqa: PLC0206

		# Ancestry was already resolved
		if fname in ancestry_map:
			continue

		# Filter by abstract marker
		if exclude_abstract and 'abstract=True' in parent_map[fname]:
			continue

		resolved = resolve_class_ancestry(fname, parent_map)

		# Filter by required parents
		if required_parents is not None and not any(f in resolved for f in required_parents):
			continue

		# Filter by excluded parents or class name
		if exclude_parents is not None:
			class_name = fname.split("/")[-1].removesuffix(".py")
			if any(f in resolved for f in exclude_parents) or class_name in exclude_parents:
				continue

		ancestry_map[fname] = resolved
		# print(fname, resolved)

	return ancestry_map
