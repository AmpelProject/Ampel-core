#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

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
    database server or the path to local catalog files. It returns a value when
    called.
    """
    @classmethod
    @abstractmethod
    def add_arguments(cls, parser):
        """
        Populate argument parser with options for this resource factory
        
        :param parser: an instance of AmpelArgumentParser
        """
        pass
    
    @abstractmethod
    def __init__(self, args):
        """
        Construct this resource factory from an argparse namespace
        
        :param args: an instance of argparse.Namespace
        """
        pass
    
    @abstractmethod
    def __call__(self):
        """
        Return the resource information. This may be either a dictionary or a
        configured object.
        """
        pass

from urllib import parse
import configargparse as argparse

class BuildURI(argparse.Action):
    def __init__(self, *args, **kwargs):
        super(BuildURI, self).__init__(*args, **kwargs)
    def __call__(self, parser, namespace, values, option_string):
        target, prop = option_string.strip('-').split('-')
        target += '_uri'
        # if not hasattr(namespace, target):
        #   setattr(namespace, target, {})
        getattr(namespace, target)[prop] = values

def uri_string(props):
    netloc = props.get('host', 'localhost')
    if 'port' in props:
        netloc += ':{}'.format(props['port'])
    if 'username' in props:
        auth = parse.quote(props['username'])
        if 'password' in props:
            auth = ':'.join((auth, parse.quote(props['password'])))
        netloc = '@'.join((auth, netloc))
    return "{}://{}/{}".format(props['scheme'], netloc, props.get('path', ''))

class ResourceURI(Resource):
    """
    A resource that can be represented as a URI
    """
    
    @classmethod
    @abstractmethod
    def name(cls):
        """
        Return a short name for this 
        """
        pass
    
    @classmethod
    @abstractmethod
    def get_default(cls):
        """
        Return a dictionary of default properties for urllib.parse.ParseResult
        """
        pass
    
    @property
    @classmethod
    def fields(cls):
        """
        Return a tuple containing the properties of urllib.parse.ParseResult
        that are required.
        """
        return 'hostname', 'port', 'username', 'password', 'path'
    
    @classmethod
    def add_arguments(cls, parser):
        group = parser.add_argument_group(cls.name, cls.__doc__)
        parser.set_defaults(**{cls.name+'_uri': cls.get_default()})
        for prop in cls.fields:
            typus = int if prop == 'hostname' else str
            group.add_argument('--{}-{}'.format(cls.name, prop), env_var='{}_{}'.format(cls.name.upper(), prop.upper()),
                action=BuildURI, type=typus, default=argparse.SUPPRESS)

    def __init__(self, args):
        key = self.name+'_uri'
        self.uri = uri_string(getattr(args, key))
        delattr(args, key)
