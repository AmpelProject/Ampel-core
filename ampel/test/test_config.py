import io
from argparse import Namespace

import pytest
import yaml

from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.config import cli


def test_validate_config():

    cb = DistConfigBuilder(verbose=False)
    cb.load_distributions()
    assert cb.build_config(stop_on_errors=False)


@pytest.mark.parametrize("doc", [{"bignumber": 1 << 57}, {1: 2}])
def test_transform_config(doc):
    """Transform preserves objects that are not representable in JSON"""
    args = Namespace(
        config_file=io.StringIO(), filter=".", output_file=io.StringIO(), validate=False
    )
    yaml.dump(doc, args.config_file)
    args.config_file.seek(0)
    cli.transform(args)
    args.output_file.seek(0)
    transformed_doc = yaml.safe_load(args.output_file)
    assert transformed_doc == doc
