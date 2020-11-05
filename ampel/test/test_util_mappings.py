from ampel.util.mappings import unflatten_dict

def test_unflatten_dict():
    assert unflatten_dict({"count.chans.HU_SN": 10}) == {
        "count": {"chans": {"HU_SN": 10}}
    }

    assert unflatten_dict(
        {
            "_id": 1,
            "tag": 1,
            "excl": 1,
            "body.jd": 1,
            "body.fid": 1,
            "body.rcid": 1,
            "body.magpsf": 1,
        }
    ) == {
        "_id": 1,
        "tag": 1,
        "excl": 1,
        "body": {"jd": 1, "fid": 1, "rcid": 1, "magpsf": 1},
    }

    assert unflatten_dict(
        {"a.0.b.f.0": 1, "a.0.b.f.1": 2, "a.0.b.f.2": 3, "a.1.c": 2, "d.e": 1},
        unflatten_list=True,
    ) == {"a": [{"b": {"f": [1, 2, 3]}}, {"c": 2}], "d": {"e": 1}}
