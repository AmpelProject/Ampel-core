#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from ampel.pipeline.config.resources import FromEnvironment
from ampel.pipeline.common.expandvars import expandvars
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from os import environ

class Graphite(FromEnvironment):
    
    name = "graphite"
    
    def __call__(self):
        import socket
        hostname = socket.gethostname().split('.')[0]
        config = dict(systemName='ampel.{}'.format(hostname))
        if 'GRAPHITE_HOST' in environ:
            config['server'] = environ['GRAPHITE_HOST']
        if 'GRAPHITE_PORT' in environ:
            config['port'] = int(environ['GRAPHITE_PORT'])
        return GraphiteFeeder(config)

class LiveMongoURI(FromEnvironment):
    
    name = "mongo"
    
    def __call__(self):
        return {'uri': expandvars("mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO}")}