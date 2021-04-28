#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/setup.py
# License           : BSD-3-Clause
# Author            : jvs/vb
# Date              : 12.10.2019
# Last Modified Date: 28.03.2021
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from setuptools import setup, find_namespace_packages

setup(
	name='ampel-core',
	version="0.7.1",
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
	entry_points = {
		'console_scripts': [
			'ampel-controller = ampel.core.AmpelController:AmpelController.main',
			'ampel-config = ampel.config.cli:main',
			'ampel-db = ampel.db.AmpelDB:main',
		],
	},
	python_requires='>=3.8,<4.0',
)

