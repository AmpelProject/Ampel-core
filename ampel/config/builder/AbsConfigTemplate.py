#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/builder/AbsConfigTemplate.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.04.2020
# Last Modified Date:  18.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelBaseModel import AmpelBaseModel


class AbsConfigTemplate(AmpelABC, AmpelBaseModel, abstract=True):
	""" Known direct subclasses: AbsProcessTemplate, AbsChannelTemplate """

	template: None | str
