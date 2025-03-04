#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/AbsMongoIngester.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.03.2020
# Last Modified Date:  09.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.abstract.AbsDocIngester import AbsDocIngester, T
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer


class AbsMongoIngester(AbsDocIngester[T], abstract=True):

	updates_buffer: DBUpdatesBuffer
