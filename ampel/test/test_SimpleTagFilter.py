import pytest

from ampel.model.DPSelection import DPSelection
from ampel.model.UnitModel import UnitModel


@pytest.mark.parametrize(
    ("filter","count"),
    [
        (None, 0),
        ("SimpleTagFilter", 5),
        (UnitModel[str](unit="SimpleTagFilter", config={"require": ["good"]}), 2),
        (UnitModel[str](unit="SimpleTagFilter", config={"forbid": ["bad"]}), 3),
        (
            UnitModel[str](
                unit="SimpleTagFilter", config={"require": ["good"], "forbid": ["bad"]}
            ),
            1,
        ),
    ],
    ids=["none", "noop", "require", "forbid", "both"],
)
def test_SimpleTagFilter(mock_context, filter, count):
    dp = DPSelection(filter=filter)
    filterer, sorter, slicer = dp.tools()
    if filter is None:
        assert filterer is None
    else:
        datapoints = [
            {"id": -2},
            {"id": -1, "tag": []},
            {"id": 0, "tag": ["good"]},
            {"id": 1, "tag": ["bad"]},
            {"id": 2, "tag": ["bad", "good"]},
        ]

        assert len(filterer.apply(datapoints)) == count
