import pytest

from ampel.model.job.JobModel import JobModel
from ampel.model.job.ExpressionParser import ExpressionParser


def test_resolve_parameters():
    model_dict = {
        "name": "job",
        "task": [
            {
                "unit": "flerp",
                "config": {"param": "foo{{ workflow.parameters.param }}bar"},
            }
        ],
        "parameters": [
            {"name": "param", "value": "-biz-"},
        ],
    }
    assert JobModel(**model_dict).task[0].config["param"] == "foo-biz-bar"


def test_evaluate_expression():
    with pytest.raises(ValueError):
        ExpressionParser.evaluate("workflow.parameters.param", {})
        ExpressionParser.evaluate("thing.parameters.param", {})
        ExpressionParser.evaluate("parameters.param", {})
    assert (
        ExpressionParser.evaluate("workflow.parameters.param", {"param": "foo"})
        == "foo"
    )
