from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, TypedDict
from typing_extensions import Required
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.content.MetaRecord import MetaRecord


class HasMeta(TypedDict, total=False):
    """
    A document with a meta entry
    """

    meta: Required[Sequence[MetaRecord]]


class MongoTTLBase(AmpelBaseModel):

    ttl: None | int = None

    def expire_at(self, doc: HasMeta) -> dict[str, Any]:
        return self.get_expire_clause(doc["meta"][-1].get("ts"))
    
    def get_expire_clause(self, now: float | int | None) -> dict[str, Any]:
        if self.ttl is not None and now is not None:
            return {"_expire_at": datetime.fromtimestamp(now + self.ttl, tz=timezone.utc)}
        else:
            return {}
