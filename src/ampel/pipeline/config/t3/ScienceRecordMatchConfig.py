
from typing import Dict, Optional, Any
from pydantic import BaseModel, validator
import pkg_resources

from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelConfig import AmpelConfig

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
                cls.print_and_raise(
                    header="select->scienceRecords->match config error",
                    msg='unknown operator {}'.format(value)
                )
        elif isinstance(value, dict):
            for k,v in value.items():
                cls.validate_match(k)
                cls.validate_match(v)
        elif isinstance(value, list):
            for v in value:
                cls.validate_match(v)
        return value

    @validator('runConfig')
    def validate_config(cls, v, values, config, field):
        configs = set()
        for ep in pkg_resources.iter_entry_points('ampel.pipeline.t2.configs'):
            for config in ep.resolve()().values():
                if config['t2Unit'] == values['unitId']:
                    configs.add(config['runConfig'])
        if not configs:
            cls.print_and_raise(
                header="select->scienceRecords->unitId config error",
                msg="Unknown T2 unit: '%s'" %  values['unitId']
            )
        if not v in configs:
            cls.print_and_raise(
                header="select->scienceRecords->runConfig config error",
                msg="Unknown {} config: '{}'\n".format(values['unitId'], v) +
                    "Valid choices are: {}".format(configs)
            )
        return v