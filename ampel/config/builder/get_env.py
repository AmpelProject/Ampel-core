#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/builder/get_env.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                Undefined
# Last Modified Date:  23.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys, importlib
from pip._internal.operations import freeze

if __name__ == "__main__":

	exceptions = {

		# Irrevelant
		"typing-extensions", "typing_extensions",
		"prometheus-client", "certifi", "six",

		# Global (added for each unit)
		"pydantic", "pymongo", "ujson", "xxhash"
	}

	pip_env = {
		z[0].replace("-", "_"): z[1]
		for el in freeze.freeze()
		if len(z := el.split("==")) > 1
		and z[0] not in exceptions
	}

	start = set(sys.modules.keys())
	importlib.import_module(sys.argv[1])
	diff = set(sys.modules.keys()) - start

	print(
		{
			m: pip_env[m]
			for k in sorted(diff)
			if (m := k.split(".")[0]) in pip_env
		}
	)
