Contribution guidelines
=======================


Naming convention
-----------------

- Classes: CamelCase
- Methods: snake_case
- Instance variables: snake_case
- Dict keys: camelCase


File structure
--------------

- One class per file (PEP 20: _Explicit is better than implicit_)


File content
------------

- Use type hints
- Prefer tab over space
- Every file should contain a header. Example:
```
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.01.2018
# Last Modified Date: 22.08.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
```
- Classes and methods should feature docstrings, preferably using reST style:
```
"""
:param param1: this is a first param
:param param2: this is a second param
:returns: description
:raises keyError: raises an exception
"""
```
