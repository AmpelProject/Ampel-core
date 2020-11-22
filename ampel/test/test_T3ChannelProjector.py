import pickle
from pathlib import Path

import pytest

from ampel.content.StockRecord import StockRecord
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.log.AmpelLogger import AmpelLogger
from ampel.t3.run.project.T3ChannelProjector import T3ChannelProjector


@pytest.fixture
def stock_record():
    with open(Path(__file__).parent / "test-data" / "ZTF20abxvcrk.pkl", "rb") as f:
        return StockRecord(pickle.load(f))


@pytest.fixture
def logger():
    return AmpelLogger.get_logger()


def strip_channel(jentries):
    return [{k: v for k, v in jentry.items() if k != "channel"} for jentry in jentries]


def test_single_channel(stock_record, logger):
    target = "TDE_RANKING"
    before = AmpelBuffer(id=stock_record["_id"], stock=stock_record)
    all_channels = set(before["stock"]["channel"])
    assert target in all_channels
    assert len(all_channels) > 1

    proj = T3ChannelProjector(channel=target, logger=logger)
    after = proj.project([before])[0]
    assert after["stock"]["channel"] == target
    assert after["stock"]["modified"] == {target: before["stock"]["modified"][target]}

    before_no_channel = strip_channel(before["stock"]["journal"])
    after_no_channel = strip_channel(after["stock"]["journal"])

    # all entries have only the selected channel
    for jentry in after["stock"]["journal"]:
        if "channel" in jentry:
            assert jentry["channel"] == target

    for jentry, stripped in zip(before["stock"]["journal"], before_no_channel):
        if "channel" in jentry:
            try:
                after_no_channel.index(stripped), "entry with channel was preserved"
            except ValueError:
                if target in jentry["channel"]:
                    raise
