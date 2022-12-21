Contribution guidelines
=======================

Structure
---------

- One class per file (PEP 20: _Explicit is better than implicit_)


Content
-------

- Please try to type hint your code and fix potential issues using _mypy_
- Prefer tab over space
- Files should contain a header. Example:
```
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Controller.py
# License           : BSD-3-Clause
# Author            : Your Name <Optional email>
# Date              : 25.01.2018
# Last Modified Date: 22.08.2018
# Last Modified By  : Your Name <Optional email>
```


Docstring
---------
- Classes and methods should include docstrings,
  preferably using reST style:
```
"""
:param param1: this is a first param
:param param2: this is a second param
:returns: description
:raises keyError: raises an exception
"""
```

Naming convention
-----------------

- Classes: CamelCase
- Methods: snake_case
- Instance variables: snake_case
- Dict keys: lowercase

Commits
-------

- Commit often
- Please write good commit messages (see _jvs_ commits for examples of high quality messages) 
