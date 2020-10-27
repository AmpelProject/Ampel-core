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

To develop and test a contributed plug-in, you will need to install the Ampel Python packages and their dependencies from within your environment. A basic set of packages for working with ZTF alerts is:

.. code-block:: shell-session
  
  pip install \
  -e git+https://github.com/AmpelProject/Ampel-interface.git#egg=ampel-interface \
  -e git+https://github.com/AmpelProject/Ampel-core.git#egg=ampel-core \
  -e git+https://github.com/AmpelProject/Ampel-alerts.git#egg=ampel-alerts \
  -e git+https://github.com/AmpelProject/Ampel-photometry.git#egg=ampel-photometry \
  -e git+https://github.com/AmpelProject/Ampel-ZTF.git#egg=ampel-ztf

.. note:: Eventually these should be published to PyPI.

.. note:: If you have two-factor authentication enabled on your GitHub account, your won't be able to use your account password to authenticate with command-line ``git``. Instead, create a `personal access token <https://github.com/settings/tokens/new>`_ with ``repo`` scope, and use that as your password. If you had previously stored your account password in a credential helper (e.g. if ``git config --system --get credential.helper`` says ``osxkeychain``), then you will have to remove the password from that credential helper before ``git`` will ask you for a new one.

.. note:: Since Ampel packages are `PEP 420 namespace packages <https://packaging.python.org/guides/packaging-namespace-packages/#creating-a-namespace-package>`_, ``python setup.py install`` will not work. Use ``pip install .`` instead.

Extra packages for ZTF alerts
=============================

If you are developing a plugin that depends on ``catsHTM`` catalog matching, e.g. via a subclass of :class:`~ampel.contrib.hu.t0.DecentFilter.DecentFilter`, you will also need to:

.. code-block:: shell-session
  
  pip install \
  -e git+https://github.com/AmpelProject/Ampel-contrib-HU.git#egg=ampel-contrib-hu

.. note:: Commonly-used elements of ``ampel-contrib-hu`` should eventually be moved up into ``ampel-ztf``.

Installing your own plug-in
===========================

Once you have the environment set up with Ampel core pacakges, you can install your own plugin (see :ref:`contributing`) in the environment with:

.. code-block:: shell-session
  
  git clone https://github.com/SomeOrganization/Ampel-contrib-PROJECTNAME.git
  cd ampel-contrib-PROJECTNAME
  pip install -e .

After this, you should be able to run :code:`ampel-config build` and see a gigantic blob of YAML being generated.

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


