#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/setup.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                Unspecified
# Last Modified Date:  21.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from setuptools import setup, find_namespace_packages

package_data = {
	'': ['*.json', 'py.typed'],
	'conf': [
		'ampel-core/*.yaml', 'ampel-core/*.yml', 'ampel-core/*.json',
		'ampel-core/**/*.yaml', 'ampel-core/**/*.yml', 'ampel-core/**/*.json',
	],
	'ampel.test': [
		'test-data/*.json',
		'deploy/production/initdb/*/*.sql',
		'deploy/prodution/initdb/*/*.sh'
	],
}

install_requires = [
	'PyYAML>=5.4.1,<7.0.0',
	'prometheus-client>=0.9.0,<=0.12',
	'psutil>=5.8.0,<6.0.0',
	'pydantic>=1.8,<2',
	'pymongo>=3.10,<5.0',
	'schedule>=1.0.0,<2.0.0',
	'sjcl>=0.2.1,<0.3.0',
	'slackclient>=2.7,<3.0',
	'yq>=2.12.0,<3.0.0',
	'ujson',
	'appdirs'
]

extras_require = {
	'docs': [
		'Sphinx>=3.5.1,<4.0.0',
		'sphinx-press-theme>=0.5.1,<0.6.0',
		'sphinx-autodoc-typehints>=1.11.1,<2.0.0',
		'tomlkit>=0.7.0,<0.8.0'
	],
	'server': [
		'fastapi>=0.63.0,<0.64.0',
		'uvicorn[standard]>=0.13.3,<0.16.0'
	]
}

entry_points = {
	'console_scripts': [
		'ampel = ampel.cli.main:main',
		'ampel-config = ampel.config.cli:main',
		'ampel-controller = '
		'ampel.core.AmpelController:AmpelController.main',
		'ampel-db = ampel.core.AmpelDB:main'
	],
	'cli': [
		'job Run provided job file = ampel.cli.JobCommand',
		'run Run selected process(es) from config = ampel.cli.RunCommand',
		'log Select, format and either view or save logs. Tail mode availabe = ampel.cli.LogCommand',
		'view Select, load and save fresh "ampel views" = ampel.cli.ViewCommand',
		'db Initialize, dump, delete specific databases or collections = ampel.cli.DBCommand',
		'config Build or update config. Fetch or append config elements = ampel.cli.ConfigCommand',
		'start Run ampel continuously. Processes are scheduled according to config = ampel.cli.StartCommand',
		't2 Match and either reset or view raw t2 documents = ampel.cli.T2Command',
		'buffer Match and view or save ampel buffers = ampel.cli.BufferCommand'
	]
}

setup(
    name = 'ampel-core',
    version = '0.8.3.alpha-8',
    description = 'Asynchronous and Modular Platform with Execution Layers',
    author = 'Valery Brinnel',
    maintainer = 'Jakob van Santen',
    maintainer_email = 'jakob.van.santen@desy.de',
    url = 'https://ampelproject.github.io',
    zip_safe = False,
    packages = find_namespace_packages(),
    package_data = package_data,
    install_requires = install_requires,
    extras_require = extras_require,
    entry_points = entry_points,
    python_requires = '>=3.10,<4.0'
)
