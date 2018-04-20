
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
    def alerts():
        """
        Generate alerts, filtering out anonymous photopoints (entries in
        prv_candidates with no candid) and photopoints that appear to come
        from an alternate pipeline where `isdiffpos` is 1/0 instead of t/f, and
        `pdiffimfilename` is an absolute path in /ztf/archive rather than a
        plain filename
        """
        parent = os.path.dirname(os.path.realpath(__file__)) + '/../'

        for fname in sorted(glob(parent+'alerts/real/*.avro')):
            with open(fname, 'rb') as f:
                
                for alert in fastavro.reader(f):
                    def valid(c):
                        if c['candid'] is None:
                            return False
                        elif c['isdiffpos'] is not None and c['isdiffpos'].isdigit():
                            return False
                        return True
                            
                    alert['prv_candidates'] = list(filter(valid, alert['prv_candidates']))
                    
                    del alert['cutoutDifference']
                    del alert['cutoutScience']
                    del alert['cutoutTemplate']
                    yield alert
    return alerts

