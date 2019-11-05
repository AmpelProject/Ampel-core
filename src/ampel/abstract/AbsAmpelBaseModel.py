#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/AbsAmpelBaseModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

# pylint: disable=unused-import
from pydantic.main import ModelMetaclass
from ampel.abstract.AmpelABC import AmpelABC
from ampel.model.AmpelBaseModel import AmpelBaseModel


class AbsAmpelBaseModel(
	AmpelBaseModel, metaclass=type('PydanticMetaWithAmpelABCMixin', (ModelMetaclass, AmpelABC), {}), 
	abstract=True
):
	""" 
	Combines metaclasses from pydantic and AmpelABC.
	Inherit this class if:
	- you want your model to be abstract
	- you wish to define/enforce abstract methods
	"""
