#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/builder/DisplayOptions.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                23.04.2022
# Last Modified Date:  23.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelBaseModel import AmpelBaseModel

class DisplayOptions(AmpelBaseModel):

	verbose: bool = False
	debug: bool = False
	hide_stderr: bool = False
	hide_module_not_found_errors: bool = False
