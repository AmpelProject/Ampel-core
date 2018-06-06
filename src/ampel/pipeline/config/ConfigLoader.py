
import json
import pkg_resources

def load_config(path):
	"""Load the JSON configuration file at path, and add plugins registered via pkg_resources"""
	with open(path) as f:
		config = json.load(f)
	for resource in pkg_resources.iter_entry_points('ampel.channels'):
		channel_config = resource.resolve()()
		name = channel_config.pop('_id')
		config['channels'][name] = channel_config
	for resource in pkg_resources.iter_entry_points('ampel.pipeline.t0'):
		klass = resource.resolve()()
		config['t0_filters'][klass.name] = dict(classFullPath=klass.__module__)
	return config
