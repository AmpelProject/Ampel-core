
import pytest
import pykafka
import subprocess
import time
import os
import tempfile
import itertools

from io import BytesIO
from glob import glob
import fastavro

def alert_blobs():
    parent = os.path.dirname(os.path.realpath(__file__)) + '/../'
    for fname in glob(parent+'alerts/ipac/*.avro'):
        with open(fname, 'rb') as f:
            yield f.read()

@pytest.fixture
def alert_generator():
    from ampel.pipeline.t0.alerts.TarballWalker import TarballWalker
    def alerts(with_schema=False):
        atat = TarballWalker('/ztf/ztf_20180419_programid2.tar.gz.part.1')
        for fileobj in itertools.islice(atat.get_files(), 0, 1000, 10):
            reader = fastavro.reader(fileobj)
            alert = next(reader)
            for k in {'cutoutDifference', 'cutoutScience', 'cutoutTemplate'}:
                 del alert[k]
            if with_schema:
                yield alert, reader.schema
            else:
                yield alert
    yield alerts


@pytest.fixture
def cutout_alert_generator():
    from ampel.pipeline.t0.alerts.TarballWalker import TarballWalker
    def alerts(with_schema=False):
        atat = TarballWalker('/ztf/cutouts/ztf_20180523_programid1.tar.gz')
        for fileobj in itertools.islice(atat.get_files(), 0, 1000, 100):
            reader = fastavro.reader(fileobj)
            alert = next(reader)
            if with_schema:
                yield alert, reader.schema
            else:
                yield alert
    yield alerts

import json
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
