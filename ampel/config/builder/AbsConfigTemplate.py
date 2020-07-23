#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/AbsConfigTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.04.2020
# Last Modified Date: 09.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.base.AmpelABC import AmpelABC
from ampel.model.StrictModel import StrictModel


class AbsConfigTemplate(AmpelABC, StrictModel, abstract=True):
	""" Known direct subclasses: AbsProcessTemplate, AbsChannelTemplate """

	template: Optional[str]
