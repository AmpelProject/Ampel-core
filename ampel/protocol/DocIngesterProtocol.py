#!/usr/bin/env python

from typing import Generic, Protocol, TypeVar

_T_contra = TypeVar("_T_contra", contravariant=True)

class DocIngesterProtocol(Protocol, Generic[_T_contra]):

	def ingest(self, doc: _T_contra) -> None:
		...

