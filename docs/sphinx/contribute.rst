Contribute
----------

.. warning:: This section is obsolete. See :ref:`legacy-porting-guide`.

Users can contribute a plugin that provides any combination of the following components:

1. A *channel*, or set of configurations for existing T0 (filter), T2 (compute), and T3 (reporting) modules.
2. A T0 module implementation.
3. A T2 module implementation.
4. A T2 configuration.
5. A T3 module implementation.
6. A T3 configuration.
7. T3 jobs.
8. Resource definitions for use in T0/T2/T3 modules

Repository layout
=================

Each Ampel plugin is kept in a separate `git` repository. These should be named after the institution or working group providing the plugin, e.g. `ampel-contrib-hu` for HU-Berlin, or `ampel-contrib-ztfbh` for the ztfbh working group. Implementation files should follow a similar naming scheme, e.g. the HU T0 module `DecentFilter` is defined in ampel/contrib/hu/t0/DecentFilter.py.

At the top level of the repository should be a `setup.py` file that `distutils` will use to install the plugin and register its components. Here is an example of a `setup.py` file for a plugin imaginatively called `ampel-contrib-example`::
  
  from distutils.core import setup

  setup(name='ampel-contrib-example',
        version='0.3.0',
        packages=['ampel.contrib.example',
                  'ampel.contrib.example.t0',
                  'ampel.contrib.example.t2',
                  'ampel.contrib.example.t3'],
        package_data = {'': ['*.json']},
        entry_points = {
            'ampel.channels' : [
                'hu = ampel.contrib.example.channels:load_channels',
            ],
            'ampel.pipeline.t0' : [
                'AwesomeFilter = ampel.contrib.example.t0.AwesomeFilter:AwesomeFilter',
            ],
            'ampel.pipeline.t2.units' : [
                'FITTYFIT = ampel.contrib.example.t2.T2FittyFit:T2FittyFit',
            ],
            'ampel.pipeline.t2.configs' : [
                'hu = ampel.contrib.example.channels:load_t2_run_configs',
            ],
            'ampel.pipeline.t3.units' : [
                'PrintyPrint = ampel.contrib.example.t3.PrintyPrint:PrintyPrint',
            ],
            'ampel.pipeline.t3.configs' : [
                'hu = ampel.contrib.example.channels:load_t3_run_configs',
            ],
            'ampel.pipeline.t3.jobs' : [
                'hu = ampel.contrib.hu.channels:load_t3_jobs',
            ],
            'ampel.pipeline.resources' : [
                'serviceyservice = ampel.contrib.example.resources:ServiceyServiceURI',
            ]
        }
  )

This plugin provides all 8 of the compoents one can provide. The `name` line provides the name of the plugin. This should match the name of the `git` repo. The `version` line lists the version, and should be incremented appropriately when changes are made to the plugin. The `packages` line lists the sub-packages that this plugin provides. The names reflect the paths to directories in the repository containing Python files. The `package_data` line instructs `distutils` to install JSON files it finds in the package directories along with Python files. The `entry_points` line registers each component with the Ampel framework.

Each of the entry points expects entries of a different type. They are:

- `ampel.channels`: Each entry should be a Python callable that returns a dictionary whose keys are channel names (all UPPERCASE by convention) and whose values are channel configurations
- `ampel.pipeline.t0`: Each entry should be a Python class that inherits from :py:class:`ampel.base.abstract.AbsAlertFilter.AbsAlertFilter`
- `ampel.pipeline.t2.units`: Each entry should be a Python class that inherits from :py:class:`ampel.base.abstract.AbsT2Unit.AbsT2Unit`
- `ampel.pipeline.t2.configs`: Each entry should be a Python callable that returns a dictionary that will be passed as the `run_config` argument of :py:class:`ampel.base.abstract.AbsT2Unit.AbsT2Unit`
- `ampel.pipeline.t3.units`: Each entry should be a Python class that inherits from :py:class:`ampel.base.abstract.AbsT3Unit.AbsT3Unit`
- `ampel.pipeline.t3.configs`: Each entry should be a Python callable that returns a dictionary that will be passed as the `run_config` argument of :py:class:`ampel.base.abstract.AbsT3Unit.AbsT3Unit`
- `ampel.pipeline.t3.jobs`: Each entry should be a Python callable that returns a dictionary whose values conform to the schema given in  :py:attr:`ampel.pipeline.t3.T3JobConfig.T3JobConfig.job_schema`
- `ampel.pipeline.resources`: Each entry should be a Python class that inherits from :py:class:`ampel.pipeline.config.resources.ResourceURI`

Add your own T2
===============

T2 modules performs science-level operations on the alerts accepted by the filter in the T0 stage. An example would be to make a template fit to the light curve of the transient or to look for more information on the transient in a set of atronomical catalogs.

The first ingredient is the T2 module itself, defining a subclass of :py:class:`ampel.base.abstract.AbsT2Unit.AbsT2Unit`. This class has two mandatory methods: ``run`` and ``_run_``. The first is just a wrapper around the second, so ``_run_`` is where the magic happens. This method requires two arguments:

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
