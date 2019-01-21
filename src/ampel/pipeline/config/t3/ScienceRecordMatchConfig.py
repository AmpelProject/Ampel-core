
from typing import Dict, Optional, Any
from pydantic import BaseModel, validator

from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.common.docstringutils import gendocstring

@gendocstring
class ScienceRecordMatchConfig(AmpelModelExtension):
    unitId: str
    runConfig: str = "default"
    match: Dict[str,Any] = {}
    
    @validator('match')
    def validate_match(cls, value):
        from mongomock.filtering import OPERATOR_MAP, LOGICAL_OPERATOR_MAP
        if isinstance(value, str) and value.startswith('$'):
            if not value in set(OPERATOR_MAP.keys()).union(LOGICAL_OPERATOR_MAP.keys()):
                raise ValueError('unknown operator {}'.format(value))
        elif isinstance(value, dict):
            for k,v in value.items():
                cls.validate_match(k)
                cls.validate_match(v)
        elif isinstance(value, list):
            for v in value:
                cls.validate_match(v)
        return value