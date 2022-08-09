from typing import Any

from ampel.abstract.AbsOpsUnit import AbsOpsUnit
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.core.EventHandler import EventHandler
from ampel.log import AmpelLogger, LogFlag, SHOUT
from ampel.log.utils import report_exception


class OpsProcessor(AbsEventUnit):

    execute: UnitModel
    update_beacon: bool = True
    log_profile: str = "console_verbose"

    def proceed(self, event_hdlr: EventHandler) -> Any:

        logger = None

        try:
            beacon_col = self.context.db.get_collection("beacon")
            logger = AmpelLogger.from_profile(
                self.context,
                self.log_profile,
                base_flag=LogFlag.CORE | self.base_log_flag,
                force_refresh=True,
            )
            last_beacon = beacon_col.find_one({"_id": self.process_name})
            beacon = self.context.loader.new_context_unit(
                model=self.execute,
                context=self.context,
                sub_type=AbsOpsUnit,
                logger=logger,
            ).run(last_beacon)
            if beacon and self.update_beacon:
                last_beacon = beacon_col.update_one(
                    {"_id": self.process_name}, {"$set": beacon}, upsert=True
                )
        except Exception as e:

            if self.raise_exc:
                raise e

            if not logger:
                logger = AmpelLogger.get_logger()

            report_exception(
                self.context.db, logger, exc=e, info={"process": self.process_name}
            )

        finally:

            if not logger:
                logger = AmpelLogger.get_logger()

            # Feedback
            logger.log(SHOUT, f"Done running {self.process_name}")
            logger.flush()
