from collections.abc import Sequence
from typing import Protocol

from bson import ObjectId

from ampel.content.JournalRecord import JournalRecord
from ampel.content.StockDocument import StockDocument
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.types import ChannelId, StockId, Tag


class StockUpdaterProtocol(Protocol):
	def add_journal_record(self,
		stock: StockId | Sequence[StockId],
		jattrs: None | JournalAttributes = None,
		tag: None | Tag | Sequence[Tag] = None,
		name: None | str | Sequence[str] = None,
		trace_id: None | dict[str, int] = None,
		action_code: None | JournalActionCode = None,
		doc_id: None | ObjectId = None,
		unit: None | int | str = None,
		channel: None | ChannelId | Sequence[ChannelId] = None,
		now: None | int | float = None
	) -> JournalRecord:
		"""
		Add a journal record to the stock document(s) identified by the input stock id(s)
		"""

	def add_name(self, stock: StockId, name: str | Sequence[str]) -> None: ...

	def add_tag(self,
		stock: StockId | Sequence[StockId],
		tag: Tag | Sequence[Tag]
	) -> None: ...

class StockIngesterProtocol(Protocol):
    def ingest(self, doc: StockDocument) -> None:
        ...
    
    @property
    def update(self) -> StockUpdaterProtocol:
        ...