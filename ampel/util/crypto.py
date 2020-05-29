#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/utils/crypto.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.04.2020
# Last Modified Date: 18.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, sys, collections
from typing import Dict, Type, TypeVar, Literal, Union, Any, Optional, List, Iterable
from ampel.model.EncryptedDataModel import EncryptedDataModel

HT = TypeVar("HT", bytes, str, int)


def b2_short_hash(payload: Union[bytes, str]) -> int:
	if isinstance(payload, str):
		payload = bytes(payload, "utf8")
	return hash_payload(payload, int, 'blake2b', digest_size=7)


def hash_payload(
	payload: bytes,
	ret: Type[HT] = bytes, # type: ignore[assignment]
	alg: Literal['sha512', 'blake2b', 'sha1', 'md5'] = 'sha512',
	**kwargs
) -> HT:
	"""
	:param ret: return type, can be bytes, str (hex digest) or int
	:param alg: hash algorithm (default is sha512)
	:param kwargs: forwarded to hashlib hash function
	"""

	# Create hash object
	ho = getattr(hashlib, alg)(payload, **kwargs)

	# using issubclass and not == because of mypy
	if issubclass(ret, bytes):
		return ho.digest()

	if issubclass(ret, int):
		return int.from_bytes(
			ho.digest(),
			byteorder=sys.byteorder
		)

	return ho.hexdigest()


def aes_recursive_decrypt(d: Dict[str, Any], pwds: Iterable[str], debug: bool = False) -> Optional[dict]:
	"""
	Note: all dicts are deep copied. Lists are copied only if they contain encrypted dicts.
	:returns: the conf entry with decrypted entries when applicable

	example:

	In []: d = {
		"mongo": {
			"writer": "mongodb://localhost:27017",
			"logger": "mongodb://localhost:27017"
		},
		"ampel-contrib-hu/slack": {
			"token": {
				"iv": "KRbaS6FY3zV+OanX2Vszxg==",
				"v": 1,
				"iter": 1000,
				"ks": 128,
				"ts": 64,
				"mode": "ccm",
				"adata": "",
				"cipher": "aes",
				"salt": "Xzur+GnyJFs=",
				"ct": "Z0OmujCT0tnubZ3wY6bX"
			},
			"channel": "#too-metachannel"
		}
	}

	In []: aes_recursive_decrypt(d, ["abc", "def", "yoyo"])
	Out[3]: {
		"mongo": {
			"writer": "mongodb://localhost:27017",
			"logger": "mongodb://localhost:27017"
		},
		"ampel-contrib-hu/slack": {
 			'token': 'bla bla',
			'channel': '#too-metachannel'
		}
	}
	"""

	if not isinstance(d, dict):
		return None

	ret: Dict[str, Any] = {}

	for k, v in d.items():

		if isinstance(v, dict):

			if 'iv' in v and 'ks' in v:

				try:
					edm = EncryptedDataModel(**v)
					ret[k] = edm.decrypt(pwds)
				except Exception:
					if debug:
						import traceback
						print("#" * 50)
						traceback.print_exc()
						print("#" * 50)
					ret[k] = v
			else:
				ret[k] = aes_recursive_decrypt(v, pwds)

			continue

		if isinstance(v, collections.abc.Sequence):
			if any(isinstance(el, dict) and 'iv' in el for el in v):
				ret[k] = [
					aes_recursive_decrypt(el, pwds) if isinstance(el, dict) else el
					for el in v
				]
				continue

		ret[k] = v

	return ret
