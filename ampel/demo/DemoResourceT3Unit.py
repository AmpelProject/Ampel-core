#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/demo/DemoResourceT3Unit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.12.2022
# Last Modified Date:  19.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from random import randint
from ampel.types import UBson
from ampel.struct.T3Store import T3Store
from ampel.struct.Resource import Resource
from ampel.struct.UnitResult import UnitResult
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit


class DemoResourceT3Unit(AbsT3PlainUnit):

	debug: bool = False

	def post_init(self) -> None:
		self.logger.info("post_init was called")

	def process(self, t3s: T3Store) -> UBson | UnitResult:
		self.logger.info("Running DemoResourceT3Unit")
		r = Resource(name='demoToken', value=randint(10000000000000, 90000000000000))
		t3s.add_resource(r)
		return UnitResult(body=r.dict()) if self.debug else None
