#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/setup.py
# License           : BSD-3-Clause
# Author            : jvs/vb
# Date              : 12.10.2019
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from setuptools import setup, find_namespace_packages

setup(
	name='ampel-core',
	version='0.7',
	packages=find_namespace_packages(),
	package_data = {
		  '': ['*.json'],
		  'ampel.test': [
			  'test-data/*.json',
			  'deploy/production/initdb/*/*.sql',
			  'deploy/prodution/initdb/*/*.sh'
		  ],
		  'conf': ['*.conf', '**/*.conf', '**/**/*.conf']
	},
	entry_points = {
		'console_scripts' : [
			'ampel-followup = ampel.t0.DelayedT0Controller:run',
			'ampel-statspublisher = ampel.metrics.AmpelStatsPublisher:run',
			'ampel-exceptionpublisher = ampel.core.AmpelExceptionPublisher:run',
			'ampel-t2 = ampel.t2.T2Controller:run',
			'ampel-t3 = ampel.t3.T3Controller:main',
			'ampel-check-broker = ampel.t0.load.fetcherutils:list_kafka',
			'ampel-archive-topic = ampel.t0.load.fetcherutils:archive_topic',
		],
		'ampel_resources' : [
			'mongo = ampel.config.resource.LiveMongoURI:LiveMongoURI',
			'graphite = ampel.config.resource.Graphite:Graphite',
			'slack = ampel.config.resource.SlackToken:SlackToken',
		]
	}
)
