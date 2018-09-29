from pydantic import BaseModel
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.T3TranSelectConfig import T3TranSelectConfig
from ampel.pipeline.config.t3.T3TranLoadConfig import T3TranLoadConfig

@gendocstring
class T3JobTranConfig(BaseModel):
	""" """
	select: T3TranSelectConfig 
	load: T3TranLoadConfig
	chunk: int = 200
