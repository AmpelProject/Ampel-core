#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/dispatchers/AbtractTransientsDispatcher.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from abc import ABC, abstractmethod


class AbtractTransientsDispatcher(ABC):

	def __init__(self):
		return

	@abstractmethod
	def dispatch(self, transient_candidate):
		return
