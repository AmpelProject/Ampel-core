
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

pytest_plugins = ['ampel.test.fixtures']

def alert_blobs():
    parent = os.path.dirname(os.path.realpath(__file__)) + '/../'
    for fname in glob(parent+'alerts/ipac/*.avro'):
        with open(fname, 'rb') as f:
            yield f.read()

@pytest.fixture
def alert_generator():
    from ampel.pipeline.t0.alerts.TarballWalker import TarballWalker
    def alerts(with_schema=False):
        atat = TarballWalker(os.path.join(os.path.dirname(__file__), '..', 'alerts', 'recent_alerts.tar.gz'))
        for fileobj in itertools.islice(atat.get_files(), 0, 1000, 1):
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


