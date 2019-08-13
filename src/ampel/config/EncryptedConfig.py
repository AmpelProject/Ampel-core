#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/EncryptedConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.11.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.common.docstringutils import gendocstring
from ampel.config.AmpelConfig import AmpelConfig

@gendocstring
class EncryptedConfig(BaseModel):
	"""
	AES encrypted config entry.
	It loads an encrypted config (dict with a given set of key/values)
	and tries to decrypt it [function get()] using passwords loaded from AmpelConfig.get_config("pwds")

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
	When Ampel starts, we populate the ampel config (class AmpelConfig).
	The used shared password *must* be a member of the list of passwords 
	returned by AmpelConfig.get_config("pwds"), otherwise decryption will fail.
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
	pwd: str = None


	def get(self):

		if not self.pwd:
			self.pwd = AmpelConfig.decrypt_config(
				self.dict()
			)

		return self.pwd
