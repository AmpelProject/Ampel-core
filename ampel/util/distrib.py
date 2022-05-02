#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/distrib.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.03.2021
# Last Modified Date: 13.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import os, re
from typing import Generator, List, Optional
from pkg_resources import ( # type: ignore[attr-defined]
	get_distribution, AvailableDistributions,
	EggInfoDistribution, DistInfoDistribution
)


def get_dist_names(distrib_prefix: str = "ampel-") -> List[str]:
	"""
	Get all installed distributions whose names start with the provided prefix
	"""
	ret = [
		dist_name for dist_name in AvailableDistributions()
		if distrib_prefix in dist_name
	]

	if ret:
		ret.insert(0, ret.pop([i for i, el in enumerate(ret) if "interface" in el][0]))
		ret.insert(0, ret.pop([i for i, el in enumerate(ret) if "core" in el][0]))

	return ret


def get_files(dist_name: str, lookup_dir: Optional[str] = None, pattern: Optional[re.Pattern] = None) -> List[str]:
	"""
	Loads all known conf files of the provided distribution (name)
	"""

	if lookup_dir and not lookup_dir.endswith("/"):
		lookup_dir += "/"

	distrib = get_distribution(dist_name)

	# DistInfoDistribution: look for metadata RECORD (in <dist_name>.dist-info)
	if isinstance(distrib, DistInfoDistribution):
		# Example of the ouput of distrib.get_metadata_lines('RECORD'):
		# 'conf/ampel-ztf.conf,sha256=FZkChNKKpcMPTO4pwyKq4WS8FAbznuR7oL9rtNYS7U0,322',
		# 'ampel/model/ZTFLegacyChannelTemplate.py,sha256=zVtv4Iry3FloofSazIFc4h8l6hhV-wpIFbX3fOW2njA,2182',
		# 'ampel/model/__pycache__/ZTFLegacyChannelTemplate.cpython-38.pyc,,',
		if pth := next(
			(
				pth for el in distrib.get_metadata_lines('RECORD')
				if (pth := el.split(",")[0]).endswith(".pth")
			),
			None
		):
			return list(walk_pth_file(pth if os.path.isfile(pth) else distrib.get_resource_filename(__name__, pth), lookup_dir, pattern))
		else:
			return [
				fname for el in distrib.get_metadata_lines('RECORD')
				if _check_match((fname := el.split(",")[0]), lookup_dir, pattern)
			]

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


def _check_match(arg: str, lookup_dir: Optional[str], pattern: Optional[re.Pattern]) -> bool:
	if lookup_dir and not arg.startswith(lookup_dir):
		return False
	if pattern and not pattern.match(arg):
		return False
	return True


def walk_pth_file(pth: str, lookup_dir: Optional[str] = None, pattern: Optional[re.Pattern] = None) -> Generator[str, None, None]:
	with open(pth) as f:
		for root, dirs, files in os.walk(f"{f.read().strip()}/{lookup_dir}" if lookup_dir else f"{f.read().strip()}"):
			for fname in files:
				if pattern and not pattern.match(fname):
					continue
				yield os.path.join(root, fname)
