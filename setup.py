#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/setup.py
# License           : BSD-3-Clause
# Author            : jvs/vb
# Date              : 12.10.2019
# Last Modified Date: 28.01.2020
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
		  'ampel': ['py.typed'],
		  'conf': ['*.conf', '**/*.conf']
	},
	entry_points = {
		'console_scripts' : [
			'ampel-followup = ampel.t0.DelayedT0Controller:run',
			'ampel-statspublisher = ampel.common.AmpelStatsPublisher:run',
			'ampel-exceptionpublisher = ampel.common.AmpelExceptionPublisher:run',
			'ampel-t2 = ampel.t2.T2Controller:run',
			'ampel-t3 = ampel.t3.T3Controller:main',
			'ampel-check-broker = ampel.t0.load.fetcherutils:list_kafka',
			'ampel-archive-topic = ampel.t0.load.fetcherutils:archive_topic',
		],
		'ampel_resources' : [
			'mongo = ampel.common.resources:LiveMongoURI',
			'graphite = ampel.common.resources:Graphite',
			'slack = ampel.common.resources:SlackToken',
		]
	}
)
