#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/getch.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                24.06.2021
# Last Modified Date:  27.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys

def yes_no(question: str) -> bool:

	try:
		while True:
			c = input(f"{question} ? [y/n]: ").lower()
			if c in ("y", "yes", "no", "n"):
				break
			sys.stdout.write('\x1b[1A')
			sys.stdout.write('\x1b[2K')
	except KeyboardInterrupt as e:
		print('\nAbording...\n\n')
		raise e

	if c == 'y' or c == 'yes':
		return True

	if c == 'n' or c == 'no':
		return False

	raise ValueError(f"Unsupported response (expected y or n): {c}")


def getch() -> bool:
	try:
		input()
	except KeyboardInterrupt:
		return True
	return False
