#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DevAmpelContext.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.06.2020
# Last Modified Date: 30.07.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.core.AmpelContext import AmpelContext
from ampel.db.AmpelDB import AmpelDB


class DevAmpelContext(AmpelContext):

	def __init__(self, db_prefix: Optional[str] = None, purge_db: bool = False, **kwargs) -> None:

		super().__init__(**kwargs)

		if db_prefix:
			dict.__setitem__(self.config._config['db'], 'prefix', db_prefix)
			self.db = AmpelDB.new(self.config)

		if purge_db:
			AmpelDB.delete_ampel_databases(self.config, db_prefix or self.config._config['db']['prefix'])
