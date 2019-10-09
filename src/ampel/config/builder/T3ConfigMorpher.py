#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/T3ConfigMorpher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 08.10.2019
# Last Modified Date: 09.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Any, Dict, Sequence

class T3ConfigMorpher:
	"""
	The sole purpose of this class is to allow users to use convenient
	shortcuts in the way they define ampel processes in channel conf files.

	T3 processes defined in channel config can be defined as follow :

	Simplified task definition examle:
        {	"tier": 3,
            "schedule": "every(30).minutes",
            "transients": {...}
            "unitId": "SlackSummaryPublisher",
            "runConfig": {...}
        }

	Internally, the following fully fledged config is used:
        {
            "tier": 3,
            "processName": "bla",
            "schedule": "every(30).minutes",
			"controller": {
				"unitId: "T3Controller"
			},
            "executor": {
				"unitId: "DefaultT3Executor"
                "runConfig": {
                    "transients": {...}
					task: [
						"name": "yoyo",
						"t3Unit": {
							"unitId": "SlackSummaryPublisher",
							"runConfig": {... }
						}
					]
                }
            }
        }

		This class aims at performing the conversion automatically
	"""

	simplest_conf_keys = ("tier", "schedule", "transients", "unitId", "runConfig")
	simple_conf_keys = ("tier", "schedule", "transients", "task")
	full_conf_keys = ("tier", "schedule", "controller", "executor")

	def __init__(self, d: Dict[str, Any]):
		""" """
		# fast copy
		self.d = json.loads(json.dumps(d))


	def has_all_keys(self, arg) -> bool:
		""" """
		dict_keys = self.d.keys()
		for el in arg:
			if el not in dict_keys:
				return False
		return True


	def get_conf(self, chan: str, pos: int = None) -> Dict[str, Any]:
		""" """

		if self.d['tier'] != 3:
			return self.d

		if self.has_all_keys(self.simplest_conf_keys):
			return self.build_from_simplest_conf(
				chan, pos, self.d['unitId'], self.d['runConfig']
			)

		elif self.has_all_keys(self.simple_conf_keys):

			if not isinstance(self.d['task'], Sequence):
				raise ValueError("Parameter task mus be a sequence. Offending value: %s" % self.d)

			if len(self.d['task']) == 1:

				if 'task' in self.d['task'][0]:
					self.d['processName'] = self.d['task'][0].pop('task')

				return self.build_from_simplest_conf(
					chan, pos, self.d['task'][0]['unitId'], self.d['task'][0]['runConfig']
				)

			else:

				if "processName" not in self.d:
					if not pos or not chan:
						raise ValueError("Cannot auto-generate process name")
					self.d['processName'] = "%s-proc%s-%s" % (
						chan, pos, "-".join([el['unitId'] for el in self.d['task']])
					)

				# Deny channel sub-selection
				for el in self.d['task']:
					if 'transients' in el:
						self.check_select(el['transients'])

				return {
					"tier": 3,
					"processName": self.d['processName'],
					"schedule": self.d['schedule'],
					"controller": {
						"unitId": "T3Controller"
					},
					"executor": {
						"unitId": "T3MultiUnitExecutor",
						"runConfig": {
							"transients": self.get_tran_conf(chan),
							"task": self.d['task']
						}
					}
				}

		elif self.has_all_keys(self.full_conf_keys):
			return self.d

		else:
			raise ValueError(
				"Unrecognized syntax found in T3 process config: %s" % self.d
			)


	def check_select(self, d):
		""" """

		if "select" not in d:
			raise ValueError("Transient selection missing. Offending dict: %s" % d)

		if "channel" in d['select']:
			raise ValueError("Channel selection not permitted here. Offending dict: %s" % d)


	def get_tran_conf(self, chan: str) -> Dict[str, Any]:
		""" """
		self.d['transients']['select']['channels'] = chan
		return self.d['transients']


	def build_from_simplest_conf(
		self, chan: str, pos: int, unit_id: str, run_config: Dict[str, Any]
	) -> Dict[str, Any]:

		if "processName" not in self.d:
			if not pos or not chan:
				raise ValueError("Cannot auto-generate process name")
			self.d['processName'] = "%s-proc%s-%s" % (chan, pos, unit_id)

		return {
			"tier": 3,
			"processName": self.d['processName'],
			"schedule": self.d['schedule'],
			"controller": {
				"unitId": "T3Controller"
			},
			"executor": {
				"unitId": "T3MonoUnitExecutor",
				"runConfig": {
					"transients": self.get_tran_conf(chan),
					"t3Unit": {
						"unitId": unit_id,
						"runConfig": run_config
					}
				}
			}
		}
