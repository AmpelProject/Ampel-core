import ast
from typing import Any, Optional


class ExpressionParser(ast.NodeVisitor):
    @classmethod
    def evaluate(cls, expression: str, context: dict) -> str:
        """Evaluate Argo-style template expressions"""
        tree = ast.parse(expression, mode="eval")
        self = cls(context)
        self.visit(tree)
        if self._value is None:
            raise ValueError(f"unable to resolve {expression}")
        return self._value

    def __init__(self, context: dict):
        self._context = context
        self._value: Optional[str] = None

    def visit_Attribute(self, node: ast.Attribute) -> None:
        path = [node.attr]
        parent = node.value
        while isinstance(parent, ast.Attribute):
            path.insert(0, parent.attr)
            parent = parent.value
        if isinstance(parent, ast.Name):
            path.insert(0, parent.id)
        context: Any = self._context
        while path:
            try:
                context = context[path.pop(0)]
            except KeyError:
                return
        if isinstance(context, str):
            self._value = context
        elif hasattr(context, "value"):
            self._value = context.value()

