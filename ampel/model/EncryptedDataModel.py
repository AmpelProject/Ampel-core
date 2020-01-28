#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/EncryptedDataModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.11.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Union
from ampel.utils.docstringutils import gendocstring

@gendocstring
class EncryptedDataModel(BaseModel):
	"""
	AES encrypted config entry.
	It loads an encrypted config (dict with a given set of key/values)
	and tries to decrypt it [function get()] using passwords loaded from AmpelConfig.get("pwds")

	To create an encrypted config, do *exactly* this:
	- go to https://bitwiseshiftleft.github.io/sjcl/demo/ 
	- enter the shared password in the green box
	- enter the secret message (authtoken for example) in the red box
	- leave authenticated data empty
	- in "Cipher Parameters", check the option CCM. (OCB2 will *not* work)
	- click on the red arrow "encrypt"
	- copy the "Ciphertext" JSON dict 
	- paste it in your config

	Make sure the 'shared password' used in step 2 is known to us. 
	When Ampel starts, an instance AmpelConfig is set up.
	The used shared password *must* be a member of the list of passwords 
	returned by ampel_config.get("pwds"), otherwise decryption will fail.
	"""

	iv: str
	v: int
	iter: int
	ks: int
	ts: int
	mode: str
	adata: str
	cipher: str
	salt: str
	ct: str

	def decrypt(self, pwds: Union[str, List[str]]) -> str:
		"""
		:raises: ValueError if no correct pwd is provided
		"""
		from sjcl import SJCL
		sjcl = SJCL()

		if isinstance(pwds, str):
			pwds = [pwds]

		for pwd in pwds:
			try:
				return sjcl \
					.decrypt(self.dict(), pwd) \
					.decode("utf-8") 
			except Exception as e:
				pass

		raise ValueError("Decryption failed, wrong password ?")
