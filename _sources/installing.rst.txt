Installing
----------

.. highlight:: console

.. _installing-environment:

Environment
===========

Ampel requires Python >= 3.8. If you already have Python 3.8 on your system, create and activate a ``virtualenv`` for Ampel with:

.. code-block:: shell-session
  
  python3 -m venv ampel-v0.7
  source ampel-v0.7/bin/activate

Otherwise, a good way to get Python 3.8 is with `miniconda <https://docs.conda.io/en/latest/miniconda.html>`_. After installing ``miniconda``, create an environment containing Python 3.8 with:

.. code-block:: shell-session
  
  conda create -y -n ampel-v0.7 python=3.8
  conda activate ampel-v0.7

Ampel packages
==============

To develop and test a contributed plug-in, you will need to install the Ampel Python packages and their dependencies from within your environment. You can install the packages needed to work with ZTF alerts from PyPI via:

.. code-block:: shell-session
  
  pip install ampel-ztf

Extra packages for ZTF alerts
=============================

If you are developing a plugin that depends on ``catsHTM`` catalog matching, e.g. via a subclass of :class:`~ampel.contrib.hu.t0.DecentFilter.DecentFilter`, you will also need to:

.. code-block:: shell-session
  
  pip install \
  -e git+https://github.com/AmpelProject/Ampel-contrib-HU.git#egg=ampel-contrib-hu

.. note:: If you have two-factor authentication enabled on your GitHub account, your won't be able to use your account password to authenticate with command-line ``git``. Instead, create a `personal access token <https://github.com/settings/tokens/new>`_ with ``repo`` scope, and use that as your password. If you had previously stored your account password in a credential helper (e.g. if ``git config --system --get credential.helper`` says ``osxkeychain``), then you will have to remove the password from that credential helper before ``git`` will ask you for a new one.

.. note:: Since Ampel packages are `PEP 420 namespace packages <https://packaging.python.org/guides/packaging-namespace-packages/#creating-a-namespace-package>`_, ``python setup.py install`` will not work. Use ``pip install .`` instead.

.. note:: Commonly-used elements of ``ampel-contrib-hu`` should eventually be moved up into ``ampel-ztf``.

Installing your own plug-in
===========================

Once you have the environment set up with Ampel core pacakges, you can install your own plugin (see :ref:`contributing`) in the environment with:

.. code-block:: shell-session
  
  git clone https://github.com/SomeOrganization/Ampel-contrib-PROJECTNAME.git
  cd ampel-contrib-PROJECTNAME
  pip install -e .

After this, you should be able to run :code:`ampel-config build` and see a gigantic blob of YAML being generated.

Development installation
========================

tl;dr: ``poetry install`` installs a single package in editable mode. ``poetry_dev path; poetry install`` in the *most-derived* repository installs it and all its (Ampel) dependencies in editable mode.

Core Ampel packages (ampel-interface, ampel-core, ampel-photometry, ampel-alerts, ampel-ztf) use `poetry <https://python-poetry.org>`_ for installation and dependency management. To install core package in development mode, change to the directory of the cloned repository and ``poetry install``. This will do the following three things:

1. Create a virtual environment for the current project. If you are already in a virtual environment, e.g. one created by ``conda create`` or ``python -m venv``, ``poetry`` will use it instead of creating a dedicated one. ``poetry env info`` will tell you which environment is being used for a particular project.
2. Install the dependencies listed in ``pyproject.toml`` into the virtual environment. If the ``poetry.lock`` file is missing or out of sync with ``pyproject.toml``, the dependencies will first be "locked" (fixed to a specific set of versions that satisfies the requirements) and written to ``poetry.lock``. Otherwise, ``poetry`` install the dependencies as given in ``poetry.lock``.
3. Install the project itself in editable mode by placing a ``.pth`` file in the ``site-packages`` directory of the virtual environment.

This ensures that all developers work with a consistent set of dependencies, and makes it trivial to publish new versions to PyPI.

``poetry`` works best for *isolated* development, where each package is developed against a fixed set of upstream dependencies. This can become cumbersome when you want to make changes across multiple Ampel core packages at once, e.g. adding an abstract base class to ampel-interface that you want to use in ampel-alerts. For this kind of development you likely want to have *all* Ampel core packages installed in editable mode at once. You can do this by replacing the `version dependencies`_ (which fetch and install packages from PyPI) in the dependent package with `path dependencies`_, pointing to a local clone of each package's repository. This process can be automated with `poetry-dev`_, as illustrated below.

.. _version dependencies: https://python-poetry.org/docs/dependency-specification/#version-constraints

.. _path dependencies: https://python-poetry.org/docs/dependency-specification/#path-dependencies

.. _poetry-dev: https://pypi.org/project/poetry-dev/

.. highlight:: shell-session

First, we create a virtualenv with ``conda`` and install ``poetry_dev`` in it. This will also install ``poetry`` itself::

  conda create -y -n ampel-installtest python=3.8; conda activate ampel-installtest
  pip install poetry_dev

Next, clone the repositories for *all* the core Ampel packages::
  
  git clone git@github.com:AmpelProject/Ampel-interface.git ampel-interface
  git clone git@github.com:AmpelProject/Ampel-core.git ampel-core
  git clone git@github.com:AmpelProject/Ampel-photometry.git ampel-photometry
  git clone git@github.com:AmpelProject/Ampel-alerts.git ampel-alerts
  git clone git@github.com:AmpelProject/Ampel-ZTF.git ampel-ztf

Then, run ``poetry_dev`` in ``ampel-ztf``, which depends on the other four. This will look for any dependencies whose names match sibling directories (which we ensured by cloning each repository as the package name), and remove them from the requirements. This will cause ``poetry`` to uninstall any distributions from PyPI that might have already been installed. Then, it will re-add these as path dependencies, causing them to be installed in develop mode:

::
  
  $ cd ampel-ztf; poetry_dev path
  ampel-interface: Changing version requirement @0.7.1-alpha.7 to path requirement../ampel-interface
  ampel-core: Changing version requirement @0.7.1-alpha.5 to path requirement../ampel-core
  ampel-photometry: Changing version requirement ~0.7.1-alpha.0 to path requirement../ampel-photometry
  ampel-alerts: Changing version requirement ~0.7.1-alpha.0 to path requirement../ampel-alerts
  Updating dependencies
  Resolving dependencies... (6.8s)
  
  Writing lock file
  
  Package operations: 36 installs, 0 updates, 0 removals
  
    • Installing mypy-extensions (0.4.3)
    • Installing pyparsing (2.4.7)
    • Installing typed-ast (1.4.2)
    • Installing typing-extensions (3.7.4.3)
    • Installing attrs (20.3.0)
    • Installing iniconfig (1.1.1)
    • Installing multidict (5.1.0)
    • Installing mypy (0.800)
    • Installing numpy (1.20.1)
    • Installing packaging (20.9)
    • Installing pluggy (0.13.1)
    • Installing py (1.10.0)
    • Installing six (1.15.0)
    • Installing toml (0.10.2)
    • Installing async-timeout (3.0.1)
    • Installing coverage (5.5)
    • Installing cycler (0.10.0)
    • Installing kiwisolver (1.3.1)
    • Installing pillow (8.1.2)
    • Installing pyerfa (1.7.2)
    • Installing pytest (6.2.2)
    • Installing python-dateutil (2.8.1)
    • Installing sentinels (1.0.0)
    • Installing yarl (1.6.3)
    • Installing aiohttp (3.7.4.post0)
    • Installing astropy (4.2)
    • Installing backoff (1.10.0)
    • Installing confluent-kafka (1.6.0)
    • Installing fastavro (1.3.4)
    • Installing matplotlib (3.3.4)
    • Installing mongomock (3.22.1)
    • Installing nest-asyncio (1.5.1)
    • Installing pytest-asyncio (0.14.0)
    • Installing pytest-cov (2.11.1)
    • Installing pytest-mock (3.5.1)
    • Installing pytest-timeout (1.4.2)
  Updating dependencies
  Resolving dependencies... (3.1s)
  
  Writing lock file
  
  Package operations: 16 installs, 0 updates, 0 removals
  
    • Installing argcomplete (1.12.2)
    • Installing pycryptodome (3.10.1)
    • Installing pydantic (1.4)
    • Installing pymongo (3.11.3)
    • Installing pyyaml (5.4.1)
    • Installing xmltodict (0.12.0)
    • Installing prometheus-client (0.9.0)
    • Installing psutil (5.8.0)
    • Installing schedule (1.0.0)
    • Installing sjcl (0.2.1)
    • Installing slackclient (2.9.3)
    • Installing yq (2.12.0)
    • Installing ampel-interface (0.7.1-alpha.8 /afs/ifh.de/group/amanda/scratch/jvsanten/projects/ztf/ampel-installtest2/ampel-interface)
    • Installing ampel-core (0.7.1-alpha.6 /afs/ifh.de/group/amanda/scratch/jvsanten/projects/ztf/ampel-installtest2/ampel-core)
    • Installing ampel-photometry (0.7.1-alpha.1 /afs/ifh.de/group/amanda/scratch/jvsanten/projects/ztf/ampel-installtest2/ampel-photometry)
    • Installing ampel-alerts (0.7.1-alpha.1 /afs/ifh.de/group/amanda/scratch/jvsanten/projects/ztf/ampel-installtest2/ampel-alerts)

.. note:: Ensure that you have *all* the Ampel core packages cloned in sibling directories before this step. If you were missing e.g. ampel-photometry, it would be installed from PyPI, which would also install its dependencies like ampel-core from PyPI.

Finally, install ``ampel-ztf`` itself in develop mode. Since all the dependencies were installed in the previous step, this does nothing but add the ``.pth`` file::

  $ poetry install
  Installing dependencies from lock file

  No dependencies to install or update

  Installing the current project: ampel-ztf (0.7.1-alpha.9)

You can verify that all packages were installed in editable mode like this::

  > ls $(python -c 'import site; print(site.getsitepackages()[0])')/ampel_*.pth
  /afs/ifh.de/group/amanda/scratch/jvsanten/software/miniconda3/envs/ampel-installtest/lib/python3.8/site-packages/ampel_alerts.pth
  /afs/ifh.de/group/amanda/scratch/jvsanten/software/miniconda3/envs/ampel-installtest/lib/python3.8/site-packages/ampel_core.pth
  /afs/ifh.de/group/amanda/scratch/jvsanten/software/miniconda3/envs/ampel-installtest/lib/python3.8/site-packages/ampel_interface.pth
  /afs/ifh.de/group/amanda/scratch/jvsanten/software/miniconda3/envs/ampel-installtest/lib/python3.8/site-packages/ampel_photometry.pth
  /afs/ifh.de/group/amanda/scratch/jvsanten/software/miniconda3/envs/ampel-installtest/lib/python3.8/site-packages/ampel_ztf.pth

.. note:: Before committing any changes to ``pyproject.toml``, run ``poetry_dev version`` to restore the PyPI dependencies. 

Running a full Ampel instance
=============================

To run the full Ampel processing chain implemented in the ampel-core project, you will need an instance of MongoDB (>= 4.0). To start one via ``docker``:

.. code-block::
  
  > docker run -d --rm -p 27017:27017 --name mongo mongo:bionic
  9f8724e53d8d4fc44ecf06e5ab5a2f76a1ad773a910ab20c2206cd3669e67496

This causes ``docker`` to start ``mongod`` in a background container named ``mongo``, with port 27017 bound to port 27017 on the host. The value printed to the console is the id of the container; you can refer to it either using this hash or the name you provided (``mongo``). You may verify that ports are forwarded to the host correctly with with ``docker inspect``:

.. code-block::
  
  > docker inspect mongo | jq '.[] | .NetworkSettings.Ports'
  {
    "27017/tcp": [
      {
        "HostIp": "0.0.0.0",
        "HostPort": "27017"
      }
    ]
  }

.. warning:: The ``mongod`` started this way has no access control whatsoever, and should only be used for local development. Always use authenticated, minimially-priviledged users in production.


