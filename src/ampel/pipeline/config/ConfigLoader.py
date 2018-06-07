#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

import json
import pkg_resources

def load_config(path):
	"""Load the JSON configuration file at path, and add plugins registered via pkg_resources"""
	with open(path) as f:
		config = json.load(f)
	for resource in pkg_resources.iter_entry_points('ampel.channels'):
		channel_config = resource.resolve()()
		name = channel_config.pop('_id')
		if name in config['channels']:
			raise KeyError("Channel config {} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
		config['channels'][name] = channel_config
	for resource in pkg_resources.iter_entry_points('ampel.pipeline.t0'):
		klass = resource.resolve()
		name = klass.__name__
		if name in config['t0_filters']:
			raise KeyError("{} (defined as entry point {} in {}) already exists in the provided config file".format(name, resource.name, resource.dist))
		config['t0_filters'][name] = dict(classFullPath=klass.__module__)

	return config
