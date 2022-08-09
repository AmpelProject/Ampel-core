#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/purge/PurgeModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.06.2020
# Last Modified Date:  18.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.purge.PurgeContentModel import PurgeContentModel
from ampel.model.purge.PurgeLogsModel import PurgeLogsModel


class PurgeModel(AmpelBaseModel):
	content: PurgeContentModel
	logs: PurgeLogsModel
