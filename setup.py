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
		'': ['*.json', 'py.typed'],
		'ampel.test': [
			'test-data/*.json',
			'deploy/production/initdb/*/*.sql',
			'deploy/prodution/initdb/*/*.sh'
		],
		'conf': [
			'*.json', '**/*.json', '**/**/*.json',
			'*.yaml', '**/*.yaml', '**/**/*.yaml',
			'*.yml', '**/*.yml', '**/**/*.yml'
		]
	},
	zip_safe=False,
	install_requires = [
		"ampel-interface",
		"typing_extensions",
		"pymongo",
		"pyyaml",
		"pydantic==1.4",
		"sjcl",
		"schedule",
		"slackclient>=2.7,<3.0",
		# install from fork, pending https://github.com/kchmck/aiopipe/pull/3
		"aiopipe @ git+https://github.com/jvansanten/aiopipe@double-close-fds#egg=aiopipe",
		"yq",
		"prometheus_client",
	],
	extras_require = {
		"testing": [
			"pytest",
			"pytest-cov",
			"pytest-asyncio",
			"mongomock",
			"coveralls",
		]
	},
	entry_points = {
		'console_scripts': [
			'ampel-controller = ampel.core.AmpelController:AmpelController.main',
			'ampel-config = ampel.config.cli:main',
			'ampel-db = ampel.db.AmpelDB:main',
			'ampel-followup = ampel.t0.DelayedT0Controller:run',
			'ampel-statspublisher = ampel.metrics.AmpelStatsPublisher:run',
			'ampel-t2 = ampel.t2.T2Controller:run',
			'ampel-t3 = ampel.t3.T3Controller:main',
			'ampel-check-broker = ampel.t0.load.fetcherutils:list_kafka',
			'ampel-archive-topic = ampel.t0.load.fetcherutils:archive_topic',
		],
		'ampel_resources': [
			'mongo = ampel.config.resource.LiveMongoURI:LiveMongoURI',
			'graphite = ampel.config.resource.Graphite:Graphite',
			'slack = ampel.config.resource.SlackToken:SlackToken',
		]
	}
)
