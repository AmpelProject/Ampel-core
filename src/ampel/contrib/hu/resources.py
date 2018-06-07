
from ampel.pipeline.config.resources import FromEnvironment
from ampel.pipeline.common.expandvars import expandvars

class extcatsURI(FromEnvironment):
    
    name = "extcats"
    
    def __call__(self):
        return {'uri': expandvars("mongodb://${CATALOG_READ_USER}:${CATALOG_READ_USER_PASSWORD}@${CATALOG}")}

class catsHTMPath(FromEnvironment):
    
    name = "catsHTM"
    
    def __call__(self):
        return {'path': os.environ['CATSHTM_PATH']}