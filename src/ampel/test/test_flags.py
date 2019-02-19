#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/test/test_flags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.01.2019
# Last Modified Date: 18.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


def test_flag_crc_collisions():
	"""
	collision example: 'codding' <-> 'gnu'
	No exception raised means everything good
	"""

	import pkg_resources, binascii

	for el in pkg_resources.iter_entry_points('ampel.pipeline.t0.sources'):

		# Implementation of ampel/core/abstract/AbsSurveySetup.py
		survey_setup = el.load()

		for flag in [
			survey_setup.get_TransientFlag(), 
			survey_setup.get_PhotoFlag(), 
			survey_setup.get_CompoundFlag(), 
			survey_setup.get_ScienceRecordFlag()
		]:

			if (
				len(set([binascii.crc32(el.encode('ascii')) for el in flag.__members__.keys()])) != 
				len(flag.__members__.keys())
			):
				from collections import defaultdict
				d = defaultdict(list)
				for el in flag:
					d[binascii.crc32(el.name.encode('ascii'))].append(el.name)
				raise ValueError(
					"Duplicated CRC hash for flag members: %s" %
					[v for k,v in d.items() if len(v) > 1]
				)

