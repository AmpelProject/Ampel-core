#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import FromEnvironment
from ampel.pipeline.common.expandvars import expandvars
from ampel.archive import ArchiveDB

class ArchiveDBWriter(FromEnvironment):
    """ArchiveDB instance with read/write access"""
    
    name = "archive_writer"
    
    def __call__(self):
        return ArchiveDB(expandvars("postgresql://${ARCHIVE_WRITE_USER}:${ARCHIVE_WRITE_USER_PASSWORD}@${ARCHIVE}/ztfarchive"))

class ArchiveDBReader(FromEnvironment):
    """ArchiveDB instance with read-only access"""
    
    name = "archive_reader"
    
    def __call__(self):
        return ArchiveDB(expandvars("postgresql://${ARCHIVE_READ_USER}:${ARCHIVE_READ_USER_PASSWORD}@${ARCHIVE}/ztfarchive"))