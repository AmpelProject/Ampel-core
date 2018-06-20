
import pytest
import json
import subprocess

def docker_service(image, protocol, port):
	container = None
	try:
		container = subprocess.check_output(['docker', 'run', '--rm', '-d', '-P', image]).strip()
		ports = json.loads(subprocess.check_output(['docker', 'container', 'inspect', '-f', '{{json .NetworkSettings.Ports}}', container]))
		yield '{}://localhost:{}'.format(protocol, ports['{}/tcp'.format(port)][0]['HostPort'])
	except FileNotFoundError:
		return pytest.skip("Docker fixture requires Docker")
	finally:
		if container is not None:
			subprocess.check_call(['docker', 'container', 'stop', container],
			    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

@pytest.fixture(scope="session")
def mongod():
	yield from docker_service('mongo:3.6', 'mongodb', 27017)

@pytest.fixture(scope="session")
def graphite():
	yield from docker_service('gographite/go-graphite:latest', 'graphite', 2003)
