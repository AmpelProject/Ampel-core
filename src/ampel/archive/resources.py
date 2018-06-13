#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import ResourceURI
from ampel.pipeline.common.expandvars import expandvars
from ampel.archive import ArchiveDB

class ArchiveDBURI(ResourceURI):
    
    fields = ('hostname', 'port', 'username', 'password')
    
    @classmethod
    def get_default(cls):
        return dict(scheme='postgresql', hostname='localhost', port=5432, path='ztfarchive')
    
    def __call__(self):
        return ArchiveDB(self.uri)

class ArchiveDBWriter(ArchiveDBURI):
    """ArchiveDB instance with read/write access"""
    
    name = "archive_writer"

class ArchiveDBReader(ArchiveDBURI):
    """ArchiveDB instance with read-only access"""
    
    name = "archive_reader"
