#!/usr/bin/env python
# File:                Ampel-core/ampel/ingest/IngestionWorker.py
# License:             BSD-3-Clause
# Author:              Jakob van Santen <jakob.van.santen@desy.de>
# Date:                19.03.2025
# Last Modified Date:  19.03.2025
# Last Modified By:    Jakob van Santen <jakob.van.santen@desy.de>

import signal

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsIngester import AbsIngester
from ampel.abstract.AbsWorker import stop_on_signal
from ampel.core.EventHandler import EventHandler
from ampel.log import AmpelLogger, LogFlag
from ampel.model.UnitModel import UnitModel
from ampel.queue.AbsConsumer import AbsConsumer
from ampel.t2.T2QueueWorker import QueueItem


class IngestionWorker(AbsEventUnit):
    consumer: UnitModel
    ingester: UnitModel = UnitModel(unit="MongoIngester")

    def proceed(self, event_hdlr: EventHandler) -> int:
        """:returns: number of t2 docs processed"""

        run_id = event_hdlr.get_run_id()

        logger = AmpelLogger.from_profile(
            self.context,
            self.log_profile,
            run_id,
            base_flag=LogFlag.CORE | self.base_log_flag,
        )

        # Loop variables
        doc_counter = 0

        with stop_on_signal(
            [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP], logger
        ) as stop_token:
            try:
                consumer = self.context.loader.new(self.consumer, unit_type=AbsConsumer)

                ingester = self.context.loader.new_context_unit(
                    self.ingester,
                    context=self.context,
                    run_id=run_id,
                    tier=-1,
                    process_name=self.process_name,
                    error_callback=stop_token.set,
                    acknowledge_callback=consumer.acknowledge,
                    logger=logger,
                    sub_type=AbsIngester,
                )

                # Process docs until next() returns None (breaks condition below)
                while not stop_token.is_set():
                    item: None | QueueItem = consumer.consume()

                    # No match
                    if item is None:
                        if not stop_token.is_set():
                            logger.log(LogFlag.SHOUT, "No more docs to process")
                        break

                    doc_counter += 1

                    with ingester.group([item]):
                        for stock in item["stock"]:
                            ingester.stock.ingest(stock)
                        for dp in item["t0"]:
                            ingester.t0.ingest(dp)
                        for t1 in item["t1"]:
                            ingester.t1.ingest(t1)
                        for t2 in item["t2"]:
                            ingester.t2.ingest(t2)

            finally:
                ingester.flush()
                event_hdlr.add_extra(docs=doc_counter)

                logger.flush()

        return doc_counter
