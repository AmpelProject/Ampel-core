#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/EncryptedConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.11.2018
# Last Modified Date: 14.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelConfig import AmpelConfig

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

if __name__ == "__main__":
	from argparse import ArgumentParser
	import json
	import re
	import secrets
	import sjcl
	parser = ArgumentParser(description="Encrypt secrets found in AMPEL configs")
	parser.add_argument("jsonfiles", nargs="+")
	parser.add_argument("outfile")
	parser.add_argument("-y", "--dry-run", action="store_true", help="print diff instead of rewriting config")
	parser.add_argument("--pattern", type=re.compile, help="regex to match secrets (e.g. xoxb- for a Slack token)")
	parser.add_argument("--passphrase", type=lambda s: bytes(s, 'utf-8'))
	args = parser.parse_args()

	encrypted_secrets = dict()

	encryptor = sjcl.SJCL()
	def encrypt(item):
		if isinstance(item, list):
			return [encrypt(i) for i in item]
		elif isinstance(item, dict):
			return {k: encrypt(v) for k,v in item.items()}
		elif isinstance(item, str) and args.pattern.match(item):
			if item in encrypted_secrets:
				crypt, passphrase = encrypted_secrets[item]
				return crypt
			else:
				passphrase = args.passphrase
			plain = item.encode('utf-8')
			crypt = encryptor.encrypt(plain, passphrase)
			assert encryptor.decrypt(crypt, passphrase) == plain
			for k in crypt:
				if isinstance(crypt[k], bytes):
					crypt[k] = crypt[k].decode()
			encrypted_secrets[item] = crypt, passphrase
			return crypt
		else:
			return item

	def print_diff(left, right, path=[]):
		if isinstance(left, dict):
			for k in left:
				print_diff(left[k], right[k], path+[k])
		elif isinstance(left, list):
			for k, v in enumerate(left):
				print_diff(left[k], right[k], path+[k])
		else:
			if left != right:
				print("{}: {} -> {}".format('.'.join(map(str,path)), left, right))

	for jsonfile in args.jsonfiles:
		with open(jsonfile, 'r') as f:
			try:
				plain = json.load(f)
			except json.decoder.JSONDecodeError as e:
				continue
		crypt = encrypt(plain)
		if crypt != plain:
			if args.dry_run:
				print(jsonfile)
				print('-'*80)
				print_diff(plain, crypt)
			else:
				with open(jsonfile, 'w') as f:
					json.dump(crypt, f, indent=2)
	if args.dry_run:
		for plain, (crypt, passphrase) in encrypted_secrets.items():
			print("{} ({}) -> {}".format(plain, passphrase, crypt))
	if encrypted_secrets:
		with open(args.outfile, 'w') as f:
			json.dump([passphrase.decode() for plain, (crypt, passphrase) in encrypted_secrets.items()], f, indent=2)
