from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from typing import Any, Literal

from pymongo.client_session import ClientSession
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

from ampel.abstract.AbsOpsUnit import AbsOpsUnit
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.content.StockDocument import StockDocument
from ampel.core.AmpelDB import AmpelDB
from ampel.log.utils import safe_query_dict
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.time.TimeConstraintModel import TimeConstraintModel
from ampel.mongo.query.stock import build_stock_query
from ampel.types import ChannelId, StockId, Tag
from ampel.util.collections import get_chunks


class StockSelectionModel(AmpelBaseModel):

    #: Select by creation time
    created: None | TimeConstraintModel = None

    #: Select by modification time
    updated: None | TimeConstraintModel = None

    #: Select by channel
    channel: None | ChannelId | AnyOf[ChannelId] | AllOf[ChannelId] | OneOf[
        ChannelId
    ] = None

    #: Select by tag
    tag: None | dict[
        Literal["with", "without"], Tag | AllOf[Tag] | AnyOf[Tag] | OneOf[Tag] | dict
    ] = None

    #: Custom selection (ex: {'run': {'$gt': 10}})
    custom: None | dict[str, Any] = None

    def get_query(self, db: AmpelDB, now: None | datetime) -> dict[str, Any]:

        # Build query for matching transients using criteria defined in config
        match_query = build_stock_query(
            channel=self.channel,
            tag=self.tag,
            time_created=self.created.get_query_model(db=db, now=now)
            if self.created
            else None,
            time_updated=self.updated.get_query_model(db=db, now=now)
            if self.updated
            else None,
        )

        if self.custom:
            match_query.update(self.custom)

        return match_query


class MongoStockDeleter(AbsOpsUnit):
    """
    Delete all documents associated with a set of stocks
    """

    #: number of stocks to purge in a single transaction
    chunk_size: int = 1000
    #: when to consider "now"
    now: Literal["latest_stock", "now"] = "latest_stock"
    #: stocks to delete
    delete: StockSelectionModel
    #: roll back each transaction before it can be committed
    dry_run: bool = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._collections = {"stock": self.context.db.get_collection("stock"),} | {
            f"t{tier}": self.context.db.get_collection(f"t{tier}") for tier in range(3)
        }

    def _purge_chunk(
        self, session: ClientSession, stock_ids: list[StockId]
    ) -> dict[str, int]:

        deleted = {k: 0 for k in self._collections}

        doc_match = {"stock": {"$in": stock_ids}}

        def delete(k):
            result = self._collections[k].delete_many(doc_match, session=session)
            self.logger.debug(
                None,
                extra={
                    "col": k,
                    "deleted": result.deleted_count if result.acknowledged else 0,
                },
            )
            return k, result.deleted_count if result.acknowledged else 0

        with ThreadPoolExecutor(max_workers=4) as executor:
            for k, v in executor.map(
                delete, [f"t{tier}" for tier in range(2, -1, -1)] + ["stock"]
            ):
                deleted[k] += v

        if self.dry_run:
            session.abort_transaction()

        self.logger.debug("Commit")

        return deleted

    def run(self, beacon: None | dict[str, Any] = None) -> None | dict[str, Any]:

        if self.now == "now":
            now = datetime.today()
        elif self.now == "latest_stock":
            # use the timestamp of the most recently inserted stock as a marker for "now"
            try:
                latest_stock: StockDocument = next(
                    self._collections["stock"]
                    .find({}, {"ts": 1})
                    .sort([("_id", -1)])
                    .limit(1)
                )
            except StopIteration:
                return None
            now = datetime.fromtimestamp(latest_stock["ts"]["any"]["tied"])
            self.logger.info(f"Last stock inserted {now} ({now.timestamp():.0f})")

        # TODO: specify conditions for stocks to delete (e.g. per channel), and then $and them
        stock_match = self.delete.get_query(self.context.db, now)

        if self.logger.verbose:
            self.logger.info(
                f"Purging {self._collections['stock'].count_documents(stock_match)} of {self._collections['stock'].estimated_document_count()} stocks",
                extra=safe_query_dict(stock_match),
            )
        else:
            self.logger.info("Purging stocks", extra=safe_query_dict(stock_match))

        deleted = {k: 0 for k in self._collections}

        with self._collections["stock"].database.client.start_session() as session:
            deleted_stocks = 0
            for docs in get_chunks(
                self._collections["stock"]
                .with_options(
                    read_concern=ReadConcern(level="local"),
                    read_preference=ReadPreference.SECONDARY_PREFERRED,
                )
                .find(
                    stock_match,
                    {"stock": 1},
                    session=session,
                ),
                self.chunk_size,
            ):
                self.logger.debug(f"Purging chunk")
                deleted_in_chunk = session.with_transaction(
                    partial(
                        self._purge_chunk, stock_ids=[doc["stock"] for doc in docs]
                    ),
                    write_concern=WriteConcern(w=1, j=True),
                )
                for k, v in deleted_in_chunk.items():
                    deleted[k] += v
                deleted_stocks += len(docs)
                self.logger.info(f"Purged {deleted_stocks} stocks", extra=deleted)

        return None