#!/usr/bin/env python
# -*- coding: utf-8 -*-
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>

from abc import ABC, abstractmethod
from ampel.pipeline.common.expandvars import expandvars
import pkg_resources

class Resource(ABC):
    """
    A resource is a property of the deploy environment, e.g. the URI of a
    database server or the path to local catalog files. It returns a value when
    called.
    """
    @classmethod
    @abstractmethod
    def add_arguments(cls, parser, defaults=None, roles=None):
        """
        Populate argument parser with options for this resource factory
        
        :param parser: an instance of AmpelArgumentParser
        :param defaults: if not None, the default configuration from the config file
        :param roles: if not None, a sequence of role names to configure
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
        parts = option_string.strip('-').split('-')
        target = parts.pop(0)
        if len(parts) == 1:
            role = None
        else:
            role = parts.pop(0)
        prop = parts.pop(0)
        assert len(parts) == 0
        target += '_uri'
        if role is None:
            getattr(namespace, target)[prop] = values
        else:
            getattr(namespace, target)['roles'][role][prop] = values

def uri_string(props):
    netloc = props.get('hostname', 'localhost')
    if 'port' in props:
        netloc += ':{}'.format(props['port'])
    if 'username' in props:
        auth = parse.quote(props['username'])
        if 'password' in props:
            auth = ':'.join((auth, parse.quote(props['password'])))
        netloc = '@'.join((auth, netloc))
    return "{}://{}/{}".format(props['scheme'], netloc, props.get('path', ''))

def render_uris(props):
    if len(props.get('roles', {})) == 0:
        return uri_string(props)
    else:
        return {k: uri_string({**props, **v}) for k,v in props['roles'].items()}

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
    
    fields = ('hostname', 'port', 'username', 'password', 'path')
    roles = tuple()
    
    @classmethod
    def add_arguments(cls, parser, defaults, roles):
        group = parser.add_argument_group(cls.name, cls.__doc__)
        default_key = cls.name+'_uri'
        class_default = cls.get_default()
        if roles is None:
            roles = cls.roles
        else:
            roles = [r for r in cls.roles if r in roles]
        class_default['roles'] = {k:{} for k in roles}
        if defaults is not None and default_key in defaults:
            superfluous = set(defaults[default_key].keys()).difference(cls.fields)
            if len(superfluous) > 0:
                raise ValueError("default configuration for {} in config file contains unrecognized keys {}".format(default_key, superfluous))
            class_default.update(defaults[default_key])
        parser.set_defaults(**{default_key: class_default})
        if 'username' in cls.fields:
            assert len(roles) == 0
        for prop in cls.fields:
            typus = int if prop == 'port' else str
            group.add_argument('--{}-{}'.format(cls.name, prop), env_var='{}_{}'.format(cls.name.upper(), prop.upper()),
                action=BuildURI, type=typus, default=argparse.SUPPRESS)
        for role in roles:
            for prop in 'username', 'password':
                group.add_argument('--{}-{}-{}'.format(cls.name, role, prop), env_var='{}_{}_{}'.format(cls.name.upper(), role.upper(), prop.upper()),
                    action=BuildURI, default=argparse.SUPPRESS)

    def __init__(self, args):
        key = self.name+'_uri'
        self.uri = render_uris(getattr(args, key))
        delattr(args, key)

    def __call__(self):
        return self.uri
