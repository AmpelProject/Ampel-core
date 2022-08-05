#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/setup.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                Unspecified
# Last Modified Date:  05.08.2022
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
	'PyYAML', 'prometheus-client', 'psutil', 'pydantic>=1.9', 'pymongo',
	'schedule', 'sjcl', 'slackclient', 'yq', 'ujson', 'appdirs'
]

extras_require = {
	'docs': ['Sphinx', 'sphinx-press-theme', 'sphinx-autodoc-typehints', 'tomlkit'],
	'server': ['fastapi', 'uvicorn[standard]']
}

entry_points = {
	'console_scripts': [
		'ampel = ampel.cli.main:main',
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
		'config Build, show or install config = ampel.cli.ConfigCommand',
		'start Run ampel continuously. Processes are scheduled according to config = ampel.cli.StartCommand',
		't2 Match and either reset or view raw t2 documents = ampel.cli.T2Command',
		'buffer Match and view or save ampel buffers = ampel.cli.BufferCommand'
	]
}

setup(
    name = 'ampel-core',
    version = '0.8.3.alpha-20',
    description = 'Alice in Modular Provenance-Enabled Land',
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
