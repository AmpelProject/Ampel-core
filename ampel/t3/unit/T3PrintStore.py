#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/unit/T3PrintStore.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.09.2022
# Last Modified Date:  26.09.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit


class T3PrintStore(AbsT3PlainUnit):

	# Otherwise logger will be used
	do_print: bool = True

	def process(self, t3s: None | T3Store = None):

		p: Any = print if self.do_print else self.logger.info
		p(f"Running {self.__class__.__name__}")

		if not t3s:
			p("No T3 store available")
			return

		p(f"Units: {t3s.units}")

		if t3s.views:
			# To be implemented
			pass

		if t3s.session:
			# To be implemented
			pass

		if t3s.extra:
			p(f"Extra: {t3s.extra}")
