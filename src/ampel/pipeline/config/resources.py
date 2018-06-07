

from abc import ABC, abstractmethod
from ampel.pipeline.common.expandvars import expandvars
import pkg_resources

def get_resource(name):
    entry = next(pkg_resources.iter_entry_points('ampel.pipeline.resources', name), None)
    if entry is not None:
        resource = entry.resolve()()
        return resource()
    else:
        raise KeyError("Resource '{}' was not defined in any package".format(name))

class Resource(ABC):
    """
    A resource is a property of the deploy environment, e.g. the URI of a
    database server or the path to local catalog files. It has a short name and
    returns a value when called.
    """
    @property
    @abstractmethod
    def name(self):
        pass
        
    @classmethod
    @abstractmethod
    def available(cls):
        pass
    
    @abstractmethod
    def __call__(self):
        """
        Return the resource information. This may be either a dictionary or a
        configured object.
        """
        pass

class FromEnvironment(Resource):
    """
    Resource information from environment variables. The resource is only
    marked as available if the required environment variables are defined.
    """
    def available(self):
        try:
            self()
            return True
        except KeyError:
            return False
