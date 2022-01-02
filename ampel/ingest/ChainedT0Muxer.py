from ampel.types import DataPointId, StockId
from ampel.content.DataPoint import DataPoint

from ampel.abstract.AbsT0Muxer import AbsT0Muxer
from ampel.model.UnitModel import UnitModel


class ChainedT0Muxer(AbsT0Muxer):
    """
    A sequence of muxers
    """

    muxers: list[UnitModel]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._muxers = [
            self.context.loader.new_context_unit(
                model=model, logger=self.logger, sub_type=AbsT0Muxer
            )
            for model in self.muxers
        ]

    def process(
        self, dps: list[DataPoint], stock_id: None | StockId = None
    ) -> tuple[None | list[DataPoint], None | list[DataPoint]]:
        """
        :returns: (to_insert,to_combine), where to_insert is the union of the insertable points
          returned by each muxer, and to_combine is the final datapoint sequence
        """
        
        dps_to_insert: dict[tuple[DataPointId, None | int], DataPoint] = dict()
        dps_to_combine: None | list[DataPoint] = dps
        
        for muxer in self._muxers:
            insert, dps_to_combine = muxer.process(dps_to_combine, stock_id)
            for dp in (insert or []):
                dps_to_insert[(dp["id"], dp.get("origin"))] = dp
        
        return list(dps_to_insert.values()) or None, dps_to_combine
