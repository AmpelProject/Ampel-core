#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/type_analysis.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 20.08.2020
# Last Modified Date: 20.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Iterable, Union, get_origin

def get_subtype(candidate_type : Any, field_type) -> Any:
    """
    Return the first element of `field_type` with which `candidate_type` is
    compatible, or None if no compabible types are found.
    This is essentially a version of issubclass() that handles typing.Union and
    returns the target type instead of just a bool.
    
    :param field_type: a type, sequence of types, or typing.Union
    """
    subfields = None
    if (origin := get_origin(field_type)) is None:
        if isinstance(field_type, Iterable) and not isinstance(field_type, (str, bytes)):
            subfields = field_type 
    elif origin is Union:
        subfields = field_type.__args__
    elif origin is candidate_type:
        # return subscripted generic
        return field_type

    if subfields:
        for subfield in subfields:
            if subtype := get_subtype(candidate_type, subfield):
                return subtype
        else:
            return None
    else:
        try:
            if issubclass(candidate_type, field_type):
                return field_type
        except TypeError:
            return None
