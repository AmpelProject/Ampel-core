#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/demo/DemoT4RunTimeAliasGenerator.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                19.12.2022
# Last Modified Date:  04.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from random import randint

from ampel.abstract.AbsT4Unit import AbsT4Unit
from ampel.struct.UnitResult import UnitResult
from ampel.types import UBson


class DemoT4RunTimeAliasGenerator(AbsT4Unit):
	""" To be run with T4RunTimeContextUpdater """

	debug: bool = False
	alias_name: str = "%%steven"

	def post_init(self) -> None:
		self.logger.info("post_init was called")

	def do(self) -> UBson | UnitResult:
		self.logger.info(f"Running {self.__class__.__name__}")
		return {self.alias_name: randint(10, 30)}
