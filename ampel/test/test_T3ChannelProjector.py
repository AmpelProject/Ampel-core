import pickle
from pathlib import Path

import pytest

from ampel.content.StockDocument import StockDocument
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.log.AmpelLogger import AmpelLogger, DEBUG
from ampel.t3.stage.project.T3ChannelProjector import T3ChannelProjector


@pytest.fixture
def stock_doc() -> StockDocument:
    with open(Path(__file__).parent / "test-data" / "ZTF20abxvcrk.pkl", "rb") as f:
        doc = pickle.load(f)
    # convert from v0.7 to v0.8
    doc["stock"] = doc["_id"]
    doc["ts"] = {
        channel: {
            "tied": doc["created"][channel],
            "upd": doc["modified"][channel],
        } for channel in doc["created"].keys() if channel != "any"
    }
    del doc["created"]
    doc["updated"] = doc.pop("modified")["any"]
    return doc


@pytest.fixture
def logger():
    return AmpelLogger.get_logger(console={"level": DEBUG})


def strip_channel(jentries):
    return [{k: v for k, v in jentry.items() if k != "channel"} for jentry in jentries]


def test_single_channel(stock_doc: StockDocument, logger):
    target = "TDE_RANKING"
    before = AmpelBuffer(id=stock_doc["stock"], stock=stock_doc)
    input_stock_doc = before["stock"]
    assert input_stock_doc is not None
    all_channels = set(input_stock_doc["channel"])
    assert target in all_channels, "target is in test case channel set"
    assert len(all_channels) > 1, "test case contains channels not in target"

    proj = T3ChannelProjector(channel=target, logger=logger)
    after = proj.project([before])[0]
    output_stock_doc = after["stock"]
    assert output_stock_doc is not None
    for field in "tag", "name", "channel":
        assert not isinstance(output_stock_doc[field], str), f"stock.{field} must be a set" # type: ignore
    assert output_stock_doc["channel"] == [target]
    assert output_stock_doc["ts"] == {target: input_stock_doc["ts"][target]}

    before_no_channel = strip_channel(input_stock_doc["journal"])
    after_no_channel = strip_channel(output_stock_doc["journal"])

    # all entries have only the selected channel
    for jentry in output_stock_doc["journal"]:
        if "channel" in jentry:
            assert jentry["channel"] == target

    for jentry, stripped in zip(input_stock_doc["journal"], before_no_channel):
        if "channel" in jentry:
            try:
                after_no_channel.index(
                    stripped
                ), "entry with target channel was preserved"
            except ValueError:
                if isinstance(jentry["channel"], int):
                    if target == jentry["channel"]:
                        raise
                elif target in jentry["channel"]:
                    raise


@pytest.mark.parametrize("logic_op", ["all_of", "any_of", "one_of"])
def test_multi_channel(stock_doc, logger, logic_op):
    target = {"RCF_2020B", "ZTF_SLSN"}
    before = AmpelBuffer(id=stock_doc["_id"], stock=stock_doc)
    all_channels = set(before["stock"]["channel"])
    assert target.intersection(all_channels), "target is in test case channel set"
    assert len(all_channels) > len(target), "test case contains channels not in target"

    proj = T3ChannelProjector(channel={logic_op: list(target)}, logger=logger)
    after = proj.project([before])[0]
    for field in "tag", "name", "channel":
        assert not isinstance(after["stock"][field], str), f"stock.{field} must be a set"
    assert set(after["stock"]["channel"]) == target
    assert after["stock"]["updated"] == before["stock"]["updated"]
    assert after["stock"]["ts"] == {k: before["stock"]["ts"][k] for k in target}

    before_no_channel = strip_channel(before["stock"]["journal"])
    after_no_channel = strip_channel(after["stock"]["journal"])

    # all entries have only the selected channel
    for jentry in after["stock"]["journal"]:
        if (channel := jentry.get("channel")) is None:
            continue
        if isinstance(channel, (int, str)):
            assert channel in target
        else:
            assert len(channel) > 1
            assert set(channel).issubset(target)

    for jentry, stripped in zip(before["stock"]["journal"], before_no_channel):
        if (channel := jentry.get("channel")) is None:
            continue
        try:
            after_no_channel.index(
                stripped
            ), "entry with channel in target was preserved"
        except ValueError:
            if (isinstance(channel, (int, str)) and channel in target) or set(
                channel
            ).issubset(target):
                raise
