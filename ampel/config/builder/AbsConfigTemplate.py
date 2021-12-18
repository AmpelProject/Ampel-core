#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/AbsConfigTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.04.2020
# Last Modified Date: 18.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelBaseModel import AmpelBaseModel


class AbsConfigTemplate(AmpelABC, AmpelBaseModel, abstract=True):
	""" Known direct subclasses: AbsProcessTemplate, AbsChannelTemplate """

	template: Optional[str]
