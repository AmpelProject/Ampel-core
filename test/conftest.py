
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

