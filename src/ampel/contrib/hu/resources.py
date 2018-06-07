#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import FromEnvironment
from ampel.pipeline.common.expandvars import expandvars

class extcatsURI(FromEnvironment):
    
    name = "extcats"
    
    def __call__(self):
        return {'uri': expandvars("mongodb://${CATALOG_READ_USER}:${CATALOG_READ_USER_PASSWORD}@${CATALOG}")}

class catsHTMPath(FromEnvironment):
    
    name = "catsHTM"
    
    def __call__(self):
        return {'path': os.environ['CATSHTM_PATH']}