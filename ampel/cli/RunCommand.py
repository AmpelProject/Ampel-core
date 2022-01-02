#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/RunCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.03.2021
# Last Modified Date:  23.03.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.core.AmpelContext import AmpelContext
from argparse import ArgumentParser
from typing import Any
from collections.abc import Sequence
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.config.AmpelConfig import AmpelConfig
from ampel.model.ProcessModel import ProcessModel
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.log.LogFlag import LogFlag


class RunCommand(AbsCoreCommand):

	def __init__(self):
		self.parser = None

	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if self.parser:
			return self.parser

		parser = AmpelArgumentParser("run")

		# Help
		parser.set_help_descr({
			"config": "Path to an ampel config file (yaml/json)",
			"process": "Process name(s) to run in the provided order (no regex)",
			"secrets": "Path to a YAML secrets store in sops format",
			"log-profile": "default, compact, headerless, verbose, debug, ..."
		})

		# Required
		parser.add_arg("config", "required", type=str)
		parser.add_arg("process", "required", nargs="+", default=None)

		# Optional
		parser.add_arg("secrets", type=str)
		parser.add_arg("log-profile", type=str, default="default")

		# Examples
		p = "ampel run --config ampel_conf.yaml "
		parser.add_example("-process process1 -log-profile verbose", prepend=p)
		parser.add_example("-process my_process -process.t0.my_process.processor.config.parameter_a 9000", prepend=p)
		parser.add_example("-process process1 process2 -db.prefix AmpelTest", prepend=p)

		self.parser = parser
		return parser


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		ctx = self.get_context(args, unknown_args, ContextClass=AmpelContext)

		pms: list[ProcessModel] = [
			pm for el in args['process']
			if (pm := get_process(ctx.config, el))
		]

		if not pms:
			print("No process found with the provided name(s)")
			return

		for i, pm in enumerate(pms):

			ctx.loader \
				.new_context_unit(
					model = pm.processor,
					context = ctx,
					process_name = pm.name,
					sub_type = AbsEventUnit,
					base_log_flag = LogFlag.MANUAL_RUN,
					log_profile = args['log_profile']
				) \
				.run()

			# Print seperator
			if i < len(pms) - 1:
				print("\n"+"="*68)


# Also used by JobCommand
def get_process(config: AmpelConfig, name: str) -> None | ProcessModel:
	for k in ("t0", "t1", "t2", "t3"):
		if (p := config.get(f"process.{k}.{name}", dict)):
			return ProcessModel(**p)
	return None
