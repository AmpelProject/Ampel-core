from typing import TypedDict, Required
from collections.abc import Sequence
from ampel.types import StockId, UnitId, T2Link

class T2DocumentMatch(TypedDict, total=False):
	"""
	Specification for identifying tier2 documents in the ampel DB.
	"""
	#: Stock id associated with the data
	stock: Required[StockId | Sequence[StockId]]

	#: Optional source origin (avoids potential stock collision between different data sources)
	origin: int

	#: Name of the unit to be run. This may be hashed for performance reasons.
	unit: Required[UnitId]

	#: Configuration hash, if unit defaults were overridden. The underlying values can be resolved with
	#: :meth:`UnitLoader.get_init_config() <ampel.core.UnitLoader.UnitLoader.get_init_config>`
	config: Required[None | int]

	#: References to input data
	link: Required[T2Link]

	#: Name of the database collection holding the input data (t1 if unspecified)
	#: (enables efficient DB queries at T3 level)
	col: str
