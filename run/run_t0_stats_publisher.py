#!/usr/bin/env python

from ampel.pipeline.common.AmpelStatsPublisher import AmpelStatsPublisher
from ampel.archive import docker_env
from os import environ

mongo_uri = 'mongodb://{}:{}@{}/'.format(
	docker_env('MONGO_INITDB_ROOT_USERNAME'),
	docker_env('MONGO_INITDB_ROOT_PASSWORD'),
	environ['MONGO']
)

asp = AmpelStatsPublisher(
	mongodb_uri=mongo_uri, 
	publish_stats=['print', 'graphite']
)
asp.start()
