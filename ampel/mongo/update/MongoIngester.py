from collections.abc import Iterable
from contextlib import contextmanager
from typing import Any

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsIngester import AbsIngester
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.protocol.StockIngesterProtocol import StockIngesterProtocol
from ampel.types import OneOrMany, Tag


class _StockIngester:

    def __init__(self, ingester: AbsDocIngester[StockDocument], updater: MongoStockUpdater) -> None:
        self.ingester = ingester
        self.update = updater
    
    def ingest(self, doc: StockDocument) -> None:
        self.ingester.ingest(doc)

class MongoIngester(AbsIngester):
    updates_buffer_size: int = 500

    #: Tag(s) to add to the stock :class:`~ampel.content.JournalRecord.JournalRecord`
    #: every time a document is processed
    jtag: None | OneOrMany[Tag]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        updates_buffer = DBUpdatesBuffer(
            self.context.db,
            self.run_id,
            self.logger,
            error_callback=self.error_callback,
            acknowledge_callback=self.acknowledge_callback,
            catch_signals=False,  # we do it ourself
            max_size=self.updates_buffer_size,
            raise_exc=self.raise_exc,
        )

        stock_updater = MongoStockUpdater(
            ampel_db = self.context.db, tier = self.tier, run_id = self.run_id,
            process_name = self.process_name, logger = self.logger,
            raise_exc = self.raise_exc, extra_tag = self.jtag

        )

        # Create ingesters
        dbconf = self.context.config.get("mongo.ingest", dict, raise_exc=True)

        def get_ingester_model(key: str) -> UnitModel:
            model = dbconf[key]
            if isinstance(model, str):
                return UnitModel(unit=model)
            return UnitModel(**model)

        self._t0 = AuxUnitRegister.new_unit(
            model=get_ingester_model("t0"),
            sub_type=AbsDocIngester[DataPoint],
            updates_buffer=updates_buffer,
        )

        self._t1 = AuxUnitRegister.new_unit(
            model=get_ingester_model("t1"),
            sub_type=AbsDocIngester[T1Document],
            updates_buffer=updates_buffer,
        )

        self._t2 = AuxUnitRegister.new_unit(
            model=get_ingester_model("t2"),
            sub_type=AbsDocIngester[T2Document],
            updates_buffer=updates_buffer,
        )

        self._stock = _StockIngester(
            AuxUnitRegister.new_unit(
                model=get_ingester_model("stock"),
                sub_type=AbsDocIngester[StockDocument],
                updates_buffer=updates_buffer,
            ),
            stock_updater,
        )

        updates_buffer.start()

        self.updates_buffer = updates_buffer

    def __del__(self) -> None:
        self.flush()

    @contextmanager
    def group(self, acknowledge_messages: None | Iterable[Any] = None):
        with self.updates_buffer.group_updates():
            yield
            for message in acknowledge_messages or []:
                self.updates_buffer.acknowledge_on_push(message)
            if len(self._stock.update._updates) >= self.updates_buffer_size:  # noqa: SLF001
                self._stock.update.flush()

    def flush(self):
        self.updates_buffer.stop()
        self.updates_buffer.push_updates(force=True)
        self._stock.update.flush()

    @property
    def stock(self) -> StockIngesterProtocol:
        return self._stock

    @property
    def t0(self) -> AbsDocIngester[DataPoint]:
        return self._t0

    @property
    def t1(self) -> AbsDocIngester[T1Document]:
        return self._t1

    @property
    def t2(self) -> AbsDocIngester[T2Document]:
        return self._t2
