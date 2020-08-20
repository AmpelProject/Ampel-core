#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/type_analysis.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 20.08.2020
# Last Modified Date: 20.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Iterable, Union

def is_subtype(candidate_type : Any, field_type) -> bool:
    """
    Return True if `candidate_type` is compatible with `field_type`.
    Essentially issubclass() that handles typing.Union.
    
    :param field_type: a type, sequence of types, or typing.Union
    return True if `maybe_type` is a subclass of cls, or a typing.Union containing one
    """
    subfields = None
    origin = getattr(field_type, '__origin__', None)
    if origin is None:
        if isinstance(field_type, Iterable):
            subfields = field_type 
    elif origin is Union:
        subfields = field_type.__args__

    if subfields:
        return any(is_subtype(candidate_type, subfield) for subfield in subfields)
    else:
        try:
            return issubclass(candidate_type, field_type)
        except TypeError:
            return False
