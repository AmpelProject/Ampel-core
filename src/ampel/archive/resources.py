#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import ResourceURI

class ArchiveDBURI(ResourceURI):
    """
    Connection to local ZTF alert database
    """
    name = 'archive'
    fields = ('hostname', 'port')
    roles = ('writer', 'reader')
    
    @classmethod
    def get_default(cls):
        return dict(scheme='postgresql', hostname='localhost', port=5432, path='ztfarchive')
