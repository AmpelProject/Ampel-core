import pytest
import pathlib
import secrets

from ampel.model.job.JobModel import JobModel
from ampel.model.job.ExpressionParser import ExpressionParser


def test_resolve_parameters():
    job = JobModel(
        **{
            "name": "job",
            "task": [
                {
                    "unit": "flerp",
                    "config": {
                        "param": "foo{{ job.parameters.param }}bar",
                        "item": "{{ item }}",
                    },
                }
            ],
            "parameters": [
                {"name": "param", "value": "-biz-"},
            ],
        }
    )
    assert job.resolve_expressions(job.task[0].dict(), job.task[0], item="scalar_item")[
        "config"
    ] == {
        "param": "foo-biz-bar",
        "item": "scalar_item",
    }


@pytest.mark.parametrize(
    ["expand_with", "result"],
    [
        ({"items": ["flim", 0, {}]}, ["flim", 0, {}]),
        ({"sequence": {"count": 1}}, ["0"]),
        ({"sequence": {"start": 1, "end": 3}}, ["1", "2"]),
    ],
)
def test_expand_with_items(expand_with, result):
    job = JobModel(
        **{
            "name": "job",
            "task": [
                {
                    "unit": "flerp",
                    "config": {
                        "item": "{{ item }}",
                    },
                    "expand_with": expand_with,
                }
            ],
        }
    )
    assert list(job.task[0].expand_with) == result


def test_multiplier_maps_to_expand():
    """
    multiplier keyword can be used as a synonym for expand_with
    """
    job = JobModel(
        **{
            "name": "job",
            "task": [
                {
                    "unit": "flerp",
                    "config": None,
                    "multiplier": 2,
                }
            ],
        }
    )
    assert list(job.task[0].expand_with) == ["0", "1"]


def test_resolve_task_outputs(tmp_path):
    token_path = tmp_path / "token"
    token_path.write_text(token := secrets.token_hex())
    job = JobModel(
        **{
            "name": "job",
            "task": [
                {
                    "unit": "bloop",
                    "config": {"output": str(token_path)},
                    "outputs": {
                        "parameters": [
                            {"name": "token", "value_from": {"path": str(token_path)}}
                        ]
                    },
                },
                {
                    "unit": "flerp",
                    "config": {"token": "{{ task.bloop.outputs.parameters.token }}"},
                },
            ],
        }
    )
    job.resolve_expressions(job.task[1].dict(), job.task[1])["config"]["token"] == token


def test_evaluate_expression():
    with pytest.raises(ValueError):
        ExpressionParser.evaluate("job.parameters.param", {})
        ExpressionParser.evaluate(
            "thing.parameters.param", {"parameters": {"param": "foo"}}
        )
        ExpressionParser.evaluate(
            "parameters.param", {"thing": {"parameters": {"param": "foo"}}}
        )
    assert (
        ExpressionParser.evaluate(
            "job.parameters.param", {"job": {"parameters": {"param": "foo"}}}
        )
        == "foo"
    )
    assert ExpressionParser.evaluate("item", {"item": "foo"}) == "foo"
