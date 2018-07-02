
import pytest
import json
import subprocess
import socket
from os.path import abspath, join, dirname

def docker_service(image, port, environ={}, mounts=[], healthcheck=None):
	container = None
	try:
		cmd = ['docker', 'run', '-d', '--restart', 'always', '-P']
		for k, v in environ.items():
			cmd += ['-e', '{}={}'.format(k,v)]
		if healthcheck is not None:
			cmd += ['--health-start-period', '1s', '--health-interval', '1s','--health-cmd', healthcheck]
		for source, dest in mounts:
			cmd += ['-v', '{}:{}'.format(source, dest)]
		container = subprocess.check_output(cmd + [image]).strip()
		
		import time
		if healthcheck is not None:
			def up():
				 status = json.loads(subprocess.check_output(['docker', 'container', 'inspect', '-f', '{{json .State.Health.Status}}', container]))
				 return status == "healthy"
			for i in range(120):
				if up():
					break
				time.sleep(1)
		ports = json.loads(subprocess.check_output(['docker', 'container', 'inspect', '-f', '{{json .NetworkSettings.Ports}}', container]))
		yield int(ports['{}/tcp'.format(port)][0]['HostPort'])
	except FileNotFoundError:
		return pytest.skip("Docker fixture requires Docker")
	finally:
		if container is not None:
			subprocess.check_call(['docker', 'container', 'stop', container],
			    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			subprocess.check_call(['docker', 'container', 'rm', container],
			    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

@pytest.fixture(scope="session")
def mongod():
	gen = docker_service('mongo:3.6', 27017)
	port = next(gen)
	yield 'mongodb://localhost:{}/'.format(port)

@pytest.fixture(scope="session")
def graphite():
	gen = docker_service('gographite/go-graphite:latest', 2003)
	port = next(gen)
	yield 'graphite://localhost:{}/'.format(port)

@pytest.fixture(scope="session")
def postgres():
	gen = docker_service('postgres:10.3', 5432,
	    environ={'POSTGRES_USER': 'ampel', 'POSTGRES_DB': 'ztfarchive', 'ARCHIVE_READ_USER': 'archive-readonly', 'ARCHIVE_WRITE_USER': 'ampel-client'},
	    mounts=[(join(abspath(dirname(__file__)), '..', '..', '..', 'deploy', 'production', 'initdb', 'archive'), '/docker-entrypoint-initdb.d/')],
	    healthcheck='psql --username ampel --port 5432 ztfarchive || exit 1')
	port = next(gen)
	yield 'postgresql://ampel@localhost:{}/ztfarchive'.format(port)
