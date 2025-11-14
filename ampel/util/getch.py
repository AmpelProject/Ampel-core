#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/util/getch.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                24.06.2021
# Last Modified Date:  27.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from rich.console import Console
from rich.prompt import Prompt

console = Console(force_terminal=True, color_system="truecolor")

def yes_no(question: str) -> bool:

	try:
		c = Prompt.ask(question, choices=["y","n"])
	except KeyboardInterrupt as e:
		print('\nAbording...\n\n')  # noqa: T201
		raise e

	if c in ('y', 'yes'):
		return True

	if c in ('n', 'no'):
		return False

	raise ValueError(f"Unsupported response (expected y or n): {c}")


def getch() -> bool:
	try:
		input()
	except KeyboardInterrupt:
		return True
	return False
