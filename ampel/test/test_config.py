
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
import pytest

def test_validate_config(mocker, mock_mongo, t3_unit_mocker):

    cb = DistConfigBuilder(verbose=False)
    cb.load_distributions()
    assert cb.build_config(ignore_errors=False)
