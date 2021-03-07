from typing import Any, Optional, Type, ClassVar, TYPE_CHECKING

from ampel.model.StrictModel import StrictModel

if TYPE_CHECKING:
	from ampel.config.AmpelConfig import AmpelConfig


class AliasableModel(StrictModel):
	"""
	A model that can be initialized from a global alias in the alias.t3 section
	of an AmpelConfig
	"""

	_config: ClassVar[Optional["AmpelConfig"]] = None

	@classmethod
	def validate(cls: Type["AliasableModel"], value: Any) -> "AliasableModel":
		if cls._config and isinstance(value, str):
			d = cls._config.get(f"alias.t3.%{value}", dict)
			if d:
				value = d
			else:
				raise ValueError(f"{cls.__name__} alias '{value}' not found in Ampel config")
		return super().validate(value)
