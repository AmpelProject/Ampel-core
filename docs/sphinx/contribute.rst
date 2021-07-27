.. _contributing:

Contribute
----------

To adapt Ampel to your needs by contributing a plugin that provides any combination of the following components:

1. A *channel*, or set of configurations for existing T0 (filter), T2 (compute), and T3 (react) modules.
2. A T0 module implementation.
3. A T2 module implementation.
4. A T3 module implementation.
5. Configuration aliases for use in T0/T2/T3 modules
6. Resource definitions for use in T0/T2/T3 modules

Repository layout
=================

Each Ampel plugin is kept in a separate ``git`` repository. These should be named after the institution or working group providing the plugin, e.g. ``ampel-contrib-hu`` for HU-Berlin, or ``ampel-contrib-ztfbh`` for the ZTFbh working group. Implementation files should follow a similar naming scheme, e.g. the HU T0 module ``DecentFilter`` is defined in ``ampel/contrib/hu/t0/DecentFilter.py``.

.. seealso:: The template repository Ampel-contrib-sample_ provides a working example of a plugin.

At the top level of the repository should be a ``setup.py`` file that ``pip install -e`` will use to install the plugin and register its components. Here is an example of a ``setup.py`` file for a plugin imaginatively called ``ampel-contrib-example``::
  
  from setuptools import setup, find_namespace_packages

  setup(
      name='ampel-contrib-sample',
      version='0.7.0',
      packages=find_namespace_packages(),
      package_data = {
          'conf': [
              '*.json', '**/*.json', '**/**/*.json',
              '*.yaml', '**/*.yaml', '**/**/*.yaml',
              '*.yml', '**/*.yml', '**/**/*.yml'
          ]
      }
  )

The ``name`` line provides the name of the plugin. This should match the name of the ``git`` repo. The ``version`` line lists the version, and should be incremented appropriately when changes are made to the plugin. The ``packages`` line specifieds the sub-packages that this plugin provides, and is generated automatically by :meth:`~setuptools.find_namespace_packages`. The ``package_data`` line instructs :mod:`setuptools` to install any YAML or JSON files it finds in the ``conf`` directory of your repository. More on that in :ref:`contribute-configuration`.

.. _contribute-configuration:

Configuration
=============

.. _contribute-configuration-channels:

Defining channels and registering units
***************************************

.. highlight:: yaml

``conf/ampel-contrib-PROJECTNAME`` should contain at least a top-level configuration file named ampel.yaml, containining at least the definitions of your channels and any custom units
you may have. For example::

  channel:
    EXAMPLE_CHANNEL:
      channel: EXAMPLE_CHANNEL
      contact: ampel@desy.de
      active: true
      auto_complete: live
  unit:
    - ampel.contrib.groupname.t0.DecentFilterCopy

The entries in `unit` are module names, i.e. :code:`ampel.contrib.groupname.t0.DecentFilterCopy` refers to the file ``ampel/contrib/groupname/t0/DecentFilterCopy.py``. This file must contain one class, with the same name as the module. For example, when :class:`~ampel.alert.AlertConsumer.AlertConsumer` requests instantiation of the unit named ``DecentFilterCopy``, the entry above will cause Ampel to do the equivalent of :code:`from ampel.contrib.groupname.t0.DecentFilterCopy import DecentFilterCopy`.

Dictionaries can be either embedded directly into the top-level configuration
file, or in standalone files named after the key. For example, `channel` key
in the example above can be replaced with a file conf/ampel-contrib-PROJECTNAME/channel/EXAMPLE_CHANNEL.yaml with the contents::
  
  channel: EXAMPLE_CHANNEL
  contact: ampel@desy.de
  active: true
  auto_complete: live

This can be useful for keeping large configurations neatly organized.

.. note:: All of the configuration files mentioned here can also be supplied in JSON_ format. We strongly recommend YAML_, however, since it is easier for a human to write and can include comments.

.. _contribute-configuration-validation:

Validation
**********

You should use the command :code:`ampel-config build` to build (and validate) an Ampel configuration file from all installed Ampel subprojects, including yours. The following examples use the ampel-contrib-sample_ template project.

You can use :code:`ampel-config build` along with yq_ to verify that your unit is registered:

.. code-block:: console
  
  > ampel-config build | yq .unit.base.DecentFilterCopy
  {
    "fqn": "ampel.contrib.groupname.t0.DecentFilterCopy",
    "base": [
      "AbsAlertFilter"
    ],
    "distrib": "ampel-contrib-sample",
    "file": "conf/ampel-contrib-sample/unit.json"
  }

This will raise an exception if your channels or T3 processes refer to units
that are not registered or can't be imported, or if your unit configurations are invalid. For example, if you add some garbage to DecentFilterCopy.py to make it non-importable, you will get:

.. code-block:: console
  
  > ampel-config build
  2020-09-24 15:52:29 AbsForwardConfigCollector:84 ERROR
   Unit import error: ampel.contrib.groupname.t0.DecentFilterCopy (conf file: conf/ampel-contrib-sample/unit.json from distribution: ampel-contrib-sample)
    Follow-up error: could not identify routing for ampel.contrib.groupname.t0.DecentFilterCopy

  2020-09-24 15:52:31 FirstPassConfig:97 WARNING
   ForwardUnitConfigCollector (key: 'unit') has errors

If you change `channel definition <https://github.com/AmpelProject/Ampel-contrib-sample/blob/03950a37dc4dc74c610df72887bd417239cd58aa/conf/ampel-contrib-sample/channel/EXAMPLE_BRIGHT_N_STABLE.yml#L11>`_  to use a unit that is not registered, for example "LALALA_DecentFilterCopy", you will get an error like this:

.. code-block:: console
  
  > ampel-config build
  2020-09-24 15:45:53 ConfigBuilder:297 ERROR
   Unable to morph embedded process EXAMPLE_BRIGHT_N_STABLE|T0|ztf_uw_public (from conf/ampel-contrib-sample/channel/EXAMPLE_BRIGHT_N_STABLE.yml)
   1 validation error for ProcessModel
  processor -> __root__ -> directives -> 0 -> filter -> __root__
    Ampel unit not found: LALALA_DecentFilterCopy (type=value_error)

If you try to configure it with parameters that are not valid, for example by `setting <https://github.com/AmpelProject/Ampel-contrib-sample/blob/03950a37dc4dc74c610df72887bd417239cd58aa/conf/ampel-contrib-sample/channel/EXAMPLE_BRIGHT_N_STABLE.yml#L13>`_ :code:`t0_filter.config.min_ndet = "fish"` when it `should be an integer <https://github.com/AmpelProject/Ampel-contrib-sample/blob/03950a37dc4dc74c610df72887bd417239cd58aa/ampel/contrib/groupname/t0/DecentFilterCopy.py#L38>`_, you get:

.. code-block:: console
  
  > ampel-config build
  2020-09-24 15:48:05 ConfigBuilder:297 ERROR
   Unable to morph embedded process EXAMPLE_BRIGHT_N_STABLE|T0|ztf_uw_public (from conf/ampel-contrib-sample/channel/EXAMPLE_BRIGHT_N_STABLE.yml)
   1 validation error for ProcessModel
  processor -> __root__ -> directives -> 0 -> filter -> __root__ -> min_ndet
  value is not a valid integer (type=type_error.integer)

.. warning:: The following sections are largely obsolete. See :ref:`legacy-porting-guide` instead.

Add your own T2
===============

T2 modules perform science-level operations on the :class:`stocks <ampel.content.StockRecord.StockRecord>`, :class:`datapoints <ampel.content.DataPoint.DataPoint>`, and :class:`compounds <ampel.content.Compound.Compound>` that are created as datapoints are added in the T0 stage. An example would be to make a template fit to the light curve of the transient or to look for more information on the transient in a set of atronomical catalogs.

The first ingredient is the T2 module itself, defining a subclass of :class:`ampel.abstract.AbsT2Unit.AbsT2Unit`. This class has two mandatory methods: ``run`` and ``_run_``. The first is just a wrapper around the second, so ``_run_`` is where the magic happens. This method requires two arguments:

.. code-block:: python

    def _run_(self, light_curve, run_config):

the first one is an instance of :py:class:`ampel.base.LightCurve` holding the transient information and photometric history. The ``run_config`` argument is a dictionary containing all the necessary parameters the job needs to run. For example, a catalog-matching T2 module will make use of the ``light_curve.get_pos`` method to compute the position of the transient and then search around this location among a set of astronomical catalogs specified in the ``run_config`` dictionary.

Once you have implemented your favourite T2, you need to register its arguments. As a given T2 module can serve many purposes depending on the parameters, i.e.: different templates in case of lightcurve fitting, different catalogs in case of coordinate matching, ecc., each of these must have be registered with its own name in the `t2_run_configs.json` file. The syntax for entries in this file is:


.. code-block:: json

    "CATALOGMATCH_sdss_class": {
        "t2Unit": "CATALOGMATCH",
        "runConfig": "sdss_class",
        "author": "ampel-info@desy.de",
        "version": 1.0,
        "lastChange": "27.08.2018",
        "private": false,
        "parameters": {
            "catalogs":{
                "SDSS_spec":{
                    "bla": "bla",
                    "bla": "bla",
                    "keys_to_append": ["z", "bptclass", "subclass"]
                }
            }
        }

In this example, we use a general purpose ``CATALOGMATCH`` T2 module to look for transient classification in the SDSS spectroscopic catalog and call this configuration ``sdss_class``. This naming of the T2 configurations makes it possible to use the T2 module simply by adding the following ``t2unit``::


    {
    "t2Unit" : "CATALOGMATCH",
    "runConfig": "sdss_class"
    }
   

to the ``t2Compute`` list of our channel configuration (the ``channels.json`` configuration file).

Configure T3 (scheduled output)
===============================

The simplest way to configure scheduled summary output for your channel is to
add a source->t3Supervise to your channel config, e.g.:

.. code-block:: json
  
  {
    "channel": "EXAMPLE",
    "sources": [
      {
        ...
        "t3Supervise": [
          {
            "task": "ExampleSummary",
            "schedule": "every().day.at('15:00')",
            "transients": {
              "select": {
                "created": {
                  "after": {
                    "use": "$timeDelta",
                    "arguments": {"days": -40}
                  }
                },
                "modified": {
                  "after": {
                    "use": "$timeLastRun",
                    "event": "ExampleSummary"
                  }
                }
                "scienceRecords": {
                  "unitId": "SNCOSMO",
                  "match": {
                    "fit_acceptable": true,
                    "sncosmo_info.success": true,
                    "fit_results.z": {"$gt": 0},
                    "fit_results.x1": {"$lt": 10}
                  }
                }
              },
              "state": "$latest",
              "content": {
                "docs": ["TRANSIENT", "COMPOUND", "T2RECORD", "PHOTOPOINT"],
                "t2SubSelection": ["SNCOSMO", "CATALOGMATCH"]
              }
            },
            "unitId": "SlackSummaryPublisher",
            "runConfig": {
              "quiet": true,
              "slackToken": "SECRETSLACKTOKEN",
              "slackChannel": "#ampel-live",
              "fullPhotometry": true
            }
          }
        ]
      }
    }

Dependencies
============

Ideally, your plugin should depend only on Python 3.6 and Ampel-base. Several other common packages are already included in the Ampel distribution. You can safely rely on:

- numpy
- astropy
- pandas
- requests

Further dependencies can be added on a case-by-case basis, provided that they are packaged with `conda` and do not conflict with the existing Ampel distribution. Heavy-weight, unpackaged, and conflicting dependencies can be supported through plugins that live in separate containers. The mechanism for this has not be completely defined yet.

Testing
=======

Write tests. Make sure they test things. Make sure they pass. See :ref:`testing`.

Getting your plugin into the main Ampel instance at DESY
========================================================

There are two ways to do this:

1. Make one of the Ampel operators (@vbrinnel or @jvansanten) a co-owner of your project. We will then take care of integrating your plugin into the build.
2. Create a special, passwordless ssh key, and communicate it to an operator. Add the public key as a (read-only) deploy key for your repository.

.. _Ampel-contrib-sample: https://github.com/AmpelProject/Ampel-contrib-sample/tree/03950a37dc4dc74c610df72887bd417239cd58aa
.. _mypy: https://mypy.readthedocs.io/en/stable/
.. _YAML: https://en.wikipedia.org/wiki/YAML
.. _JSON: https://en.wikipedia.org/wiki/JSON
.. _yq: https://mikefarah.gitbook.io/yq/

