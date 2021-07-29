import sys, importlib
from pip._internal.operations import freeze

if __name__ == "__main__":

	exceptions = {

		# Irrevelant
		"typing-extensions", "prometheus-client",
		"certifi", "six",

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
