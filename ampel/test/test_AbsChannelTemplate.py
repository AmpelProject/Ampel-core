from typing import Any
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.ChannelModel import ChannelModel

class ChannelTemplate(AbsChannelTemplate):
    def get_processes(self, logger: AmpelLogger, first_pass_config: FirstPassConfig) -> list[dict[str, Any]]:
        return []
    

def test_AbsChannelTemplate(ampel_logger):

    template = ChannelTemplate(
        channel = "FOO",
        version = 0,
    )
    channel = template.get_channel(logger=ampel_logger)
    assert ChannelModel(**channel).__dict__ == channel
    