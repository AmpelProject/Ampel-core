#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/job/ExpressionParser.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  13.08.2022
# Last Modified By:    jvs

import ast
from pathlib import Path
from typing import Any

class ExpressionParser(ast.NodeVisitor):

    @classmethod
    def evaluate(cls, expression: str, context: dict) -> str:
        """Evaluate Argo-style template expressions"""
        tree = ast.parse(expression, mode="eval")
        self = cls(context)
        self.visit(tree)
        if isinstance(self._value, str):
            return self._value
        elif isinstance(self._value, Path):
            return self._value.read_text()
        raise ValueError(f"unable to resolve {expression}")

    def __init__(self, context: dict):
        self._context = context
        self._value: Any = None
    
    def visit_Name(self, node: ast.Name) -> None:
        self.generic_visit(node)
        self._value = self._context.get(node.id)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self.generic_visit(node)
        if isinstance(self._value, dict):
            self._value = self._value.get(node.attr)
    
    def visit_Subscript(self, node: ast.Subscript) -> Any:
        self.generic_visit(node)
        if isinstance(self._value, dict):
            self._value = self._value.get(node.value)
        elif isinstance(self._value, list) and isinstance(node, ast.Num):
            self._value = self._value[node.value]
