#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/setup.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                Unspecified
# Last Modified Date:  06.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from setuptools import find_namespace_packages, setup

setup(
    name = 'ampel-core',
    version = '0.10.6a15',
    description = 'Ampel-core package',
    author = 'Valery Brinnel',
    maintainer = 'Jakob van Santen',
    maintainer_email = 'jakob.van.santen@desy.de',
    url = 'https://ampelproject.github.io',
    zip_safe = False,
    packages = find_namespace_packages(include=["ampel*"]),
    python_requires = '>=3.10,<4.0',
	data_files=[(
		"conf/ampel-core", [
            "conf/ampel-core/ampel.yaml",
            "conf/ampel-core/logging.yaml",
            "conf/ampel-core/mongo/data.yaml",
            "conf/ampel-core/mongo/ext.yaml",
            "conf/ampel-core/mongo/var.yaml"
        ]
	)],
    install_requires = [
		'PyYAML', 'prometheus-client', 'psutil', 'pydantic>=1.9', 'pymongo',
		'schedule', 'sjcl', 'slackclient', 'yq', 'ujson', 'platformdirs', 'rich'
	],
    extras_require = {
		'docs': ['Sphinx', 'sphinx-press-theme', 'sphinx-autodoc-typehints', 'tomlkit'],
		'server': ['fastapi', 'uvicorn[standard]']
	},
    entry_points = {
		'console_scripts': [
			'ampel = ampel.cli.main:main',
			'ampel-controller = '
			'ampel.core.AmpelController:AmpelController.main',
			'ampel-db = ampel.core.AmpelDB:main'
		],
		'cli': [
			'job Run job schema file(s) = ampel.cli.JobCommand',
			'run Run selected process(es) from config = ampel.cli.RunCommand',
			'log Select, format and either view (tail mode available) or save logs = ampel.cli.LogCommand',
			'view Select, load and save fresh "ampel views" = ampel.cli.ViewCommand',
			'db Initialize, dump, delete specific databases or collections = ampel.cli.DBCommand',
			'config Build, show or install config = ampel.cli.ConfigCommand',
			#'start Run ampel continuously. Processes are scheduled according to config = ampel.cli.StartCommand',
			't2 Match and either reset or view raw t2 documents = ampel.cli.T2Command',
			'buffer Match and view or save ampel buffers = ampel.cli.BufferCommand',
			'event Show events information = ampel.cli.EventCommand'
		]
	}
)
