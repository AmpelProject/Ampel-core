import ast
from typing import Optional


class ExpressionParser(ast.NodeVisitor):
    @classmethod
    def evaluate(cls, expression: str, parameters: dict[str, str]) -> str:
        """Evaluate Argo-style template expressions"""
        tree = ast.parse(expression, mode="eval")
        self = cls(parameters)
        self.visit(tree)
        if self._value is None:
            raise ValueError(f"unable to resolve {expression}")
        return self._value

    def __init__(self, parameters: dict[str, str]):
        self._parameters = parameters
        self._value : Optional[str] = None

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if (
            isinstance(node.value, ast.Attribute)
            and node.value.attr == "parameters"
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "workflow"
        ):
            self._value = self._parameters.get(node.attr)
