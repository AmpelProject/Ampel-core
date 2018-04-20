
import pytest
import pykafka
import subprocess
import time
import os
import tempfile

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
    from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
    return lambda : ZIAlertLoader.walk_tarball('/ztf/ztf_20180419_programid2.tar.gz')

