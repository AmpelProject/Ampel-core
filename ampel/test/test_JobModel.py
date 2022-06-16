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
                    "config": {"param": "foo{{ job.parameters.param }}bar"},
                }
            ],
            "parameters": [
                {"name": "param", "value": "-biz-"},
            ],
        }
    )
    job.resolve_expressions(job.task[0].dict(), job.task[0])["config"][
        "param"
    ] == "foo-biz-bar"


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
