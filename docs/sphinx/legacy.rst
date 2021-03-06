
.. _legacy-porting-guide:

Porting contrib projects from v0.6
----------------------------------

Ampel v0.7 introduced a major internal restructuring to apply lessons learned
from operating v0.6 during the first 1.5 years of ZTF. Here is a summary of the
changes you need to make to adapt your project from v0.6 to v0.7. An example
project using the current config system can be found in
`Ampel-contrib-sample`_.

.. _Ampel-contrib-sample: https://github.com/AmpelProject/Ampel-contrib-sample/tree/03950a37dc4dc74c610df72887bd417239cd58aa

Environment
===========

See :ref:`installing-environment`.

.. _legacy-config-files:

Defining channels and registering units
=======================================

.. highlight:: python

All the channel/T3 configuration and unit registration that used to be
scattered in json files and entrypoints section of setup.py now lives in YAML_
files in a directory ``conf/ampel-contrib-PROJECTNAME`` (where ``PROJECTNAME`` is the name of your project, e.g. ``ZTFbh`` for ``ampel-contrib-ZTFbh``) at the top level
of your repository. See :ref:`contribute-configuration-channels` for details of the configuration layout, and :ref:`contribute-configuration-validation` for how to check your configuration for correctness.

Terminology changes and renamed classes
***************************************

- T3Job and T3Task are no more. Everything is just a Process now.
- Many classes have been renamed to more accurately reflect their meaning. A partial list is below. Note that the names refer to the class with the same name as the module, e.g. ``ampel.view.LightCurve`` means ``from ampel.view.LightCurve import LightCurve``

====================================== ============================================
v0.6 class                             v0.7 (nearest equivalent)
====================================== ============================================
``ampel.base.LightCurve``              :class:`ampel.view.LightCurve <ampel.view.LightCurve.LightCurve>`
``ampel.base.AmpelAlert``              :class:`ampel.alert.PhotoAlert <ampel.alert.PhotoAlert.PhotoAlert>`
``ampel.base.ScienceRecord``           :class:`ampel.content.T2Record <ampel.content.T2Record.T2Record>`
``ampel.base.TransientView``           :class:`ampel.view.TransientView <ampel.view.TransientView.TransientView>`
``ampel.base.PlainPhotoPoint``         :class:`ampel.content.DataPoint <ampel.content.DataPoint.DataPoint>`
``ampel.base.PlainUpperLimit``         :class:`ampel.content.DataPoint <ampel.content.DataPoint.DataPoint>`
``ampel.base.flags.PhotoFlags``        None (replaced by :class:`data_point["tag"] <ampel.content.DataPoint.DataPoint>`)
``ampel.base.flags.TransientFlags``    None (replaced by :class:`stock_record["tag"] <ampel.content.StockRecord.StockRecord>`)
``ampel.base.dataclass.JournalUpdate`` :class:`ampel.struct.JournalExtra <ampel.struct.JournalExtra.JournalExtra>`
``ampel.base.dataclass.GlobalInfo``    None (:class:`AbsT3Unit.context <ampel.abstract.AbsT3Unit.AbsT3Unit>` is populated by instances of :class:`~ampel.t3.context.AbsT3RunContextAppender.AbsT3RunContextAppender`)
``ampel.base.abstract.AbsAlertFilter`` :class:`ampel.abstract.AbsAlertFilter[PhotoAlert] <ampel.abstract.AbsAlertFilter.AbsAlertFilter>`
``ampel.base.abstract.AbsT2Unit``      :class:`ampel.abstract.AbsLightCurveT2Unit <ampel.abstract.AbsLightCurveT2Unit.AbsLightCurveT2Unit>`
``ampel.base.abstract.AbsT3Unit``      :class:`ampel.abstract.AbsPhotoT3Unit <ampel.abstract.AbsPhotoT3Unit.AbsPhotoT3Unit>`
====================================== ============================================

- Classes in ``ampel.content`` are declared as ``TypedDict``. At runtime these are ``ReadOnlyDict``, but the annotations in the class definition tell you which keys they may have.

Channel definitions
*******************

The easiest way to define a channel is with a YAML file, e.g. conf/ampel-contrib-PROJECTNAME/channel/EXAMPLE_BRIGHT_N_STABLE.yaml. The content is similar to the JSON-based channel definitions in v0.6, but simplified. A few notable differences:

- *templates* define common configurations that don't need to be repeated in every channel definition, and replace much of the boilerplate found in v0.6 channel definitions. See :class:`~ampel.model.ZTFLegacyChannelTemplate.ZTFLegacyChannelTemplate`.
- T2 unit configurations can be defined either inline or in the `alias` section of the top-level config. A separate t2_config.json is no longer needed.
- T3 process definitions embedded in the channel definition can also use templates. See :class:`~ampel.model.ZTFPeriodicSummaryT3.ZTFPeriodicSummaryT3`.

A slightly truncated example::
  
  channel: EXAMPLE_BRIGHT_N_STABLE
  contact: ampel@desy.de
  active: true
  # Auto-complete mode: how to treat photopoints be treated once a transient has
  # been accepted.
  # - false: apply filter to all photopoints
  # - true or "live": bypass filter once a transient has been accepted once
  auto_complete: live
  # Channel template: basic settings for which alert stream to listen to, how to
  # build light curves from alert packets, etc.
  template: ztf_uw_public
  # T0: which photopoints should be accepted to build light curves for each
  #     transient?
  t0_filter:
    unit: DecentFilterCopy
    config:
      min_ndet: 2
      min_tspan: 0
      max_tspan: 5
      ...
  # T2: how should the collected photopoints and light curves be augmented?
  t2_compute:
    - unit: T2SNCosmo
      config:
        model: salt2
        upper_limits: false
    # config can be omitted if the unit has defaults
    - unit: T2ExamplePolyFit
    - unit: T2CatalogMatch
      # A named configuration, defined in alias/t2. Names that start with "%" are
      # global, other names are local to the project
      config: '%T2CatalogMatch_general'
  # T3: what should I do with the collected data?
  t3_supervise:
    # A minimal T3: select all data for transients modified since last run
    # the optional parameters `name`, `load`, `filter`, and `complement` are set
    # to sensible defaults.
    - template: ztf_periodic_summary
      schedule: every().day.at('15:00')
      run:
        unit: DemoT3Unit
    # More settings: load only transient and T2 records for transients modified
    # since last run where `sncosmo` color parameter is > 1
    - name: set_all_the_things
      template: ztf_periodic_summary
      schedule: every(4).hours
      load:
        - TRANSIENT
        - T2RECORD
      filter:
        t2:
          unit: T2SNCosmo
          match:
            fit_results.c:
              $gt: 1
      run:
        unit: DemoT3Unit

Some operations that were previously embedded in T3 units, like filtering :class:`TransientView <ampel.view.TransientView.TransientView>` in :meth:`AbsT3Unit.add <ampel.abstract.AbsT3Unit.AbsT3Unit.add>`, now have their own dedicated stages. This makes it possible to reuse these stages without writing new code.

Standalone T3 processes
***********************

Just like in v0.6, T3 processes embedded in a channel definition implicitly
select only transients associated with that channel. To consume transients from
multiple channels, you have to define a standalone T3 process. These definitions
also use templates, however, so can be quite compact::
  
  name: TNSCompleteSummary
  tier: 3
  # every 60 minutes, consume all transients that were updated since the
  # previous run in channels HU_GP_10 or HU_GU_59
  template: ztf_periodic_summary
  schedule: every(60).minutes
  channel:
    any_of:
      - HU_GP_10
      - HU_GP_59
  # load the stock, t0, and t2 records associated with the transient (and channel)
  load:
    - TRANSIENT
    - DATAPOINT
    - T2RECORD
  # for each selected transient, look up the TNS name
  complement: TNSNames
  # and pass to TNSTalker
  run:
    unit: TNSTalker
    config:
      # a Secret item, kept separate from the rest of the config
      tns_api_key:
        key: tns/jnordin
      submit_tns: true
      sandbox: false
      max_age: 30
      needed_catalogs: []

.. highlight:: python


T0 units
========

.. _legacy-t0-configuration:

T0 unit configuration
*********************

T0 units need to be :ref:`registered in your project's config <contribute-configuration-channels>`.

All units in v0.7 use type annotations and ``pydantic`` to define and validate their configuration. This means that if you previously used a nested :class:`RunConfig` class to define a configuration, you can move its fields up to the parent class, and access them as attributes from instances. In other words, the following v0.6 filter defintion::
  
  from pydantic import BaseModel
  from ampel.base.abstract.AbsAlertFilter import AbsAlertFilter

  class AwesomeFilter(AbsAlertFilter):
  
      class RunConfig(BaseModel):
          """
          Necessary class to validate configuration.
          """
          MIN_NDET: int # number of previous detections
          MIN_TSPAN: float # minimum duration of alert detection history [days]
          MAX_TSPAN: float # maximum duration of alert detection history [days]
  
      def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
          if run_config is None:
              raise ValueError("Please check your run configuration")
  
          self.on_match_t2_units = on_match_t2_units
          self.logger = logger if logger is not None else logging.getLogger()
  
          # parse the run config
          rc_dict = run_config.dict()
  
          # ----- set filter proerties ----- #
  
          # history
          self.min_ndet = rc_dict['MIN_NDET'] 
          self.min_tspan = rc_dict['MIN_TSPAN']
          self.max_tspan = rc_dict['MAX_TSPAN']

shrinks down to::
  
  from pydantic import Field
  from ampel.alert.PhotoAlert import PhotoAlert
  from ampel.abstract.AbsAlertFilter import AbsAlertFilter

  class AwesomeFilter(AbsAlertFilter[PhotoAlert]):
  
      min_ndet: int #: number of previous detections
      min_tspan: float #: minimum duration of alert detection history [days]
      max_tspan: float #: maximum duration of alert detection history [days]

      def post_init(self):
          ...

You no longer have to define an :meth:`__init__`; the default :meth:`__init__` will set ``self.min_ndet`` and raise an exception if required fields are not set or set with invalid values. If you need to do any custom setup, however, you can define a :meth:`post_init` that will be called within the base class :meth:`__init__`. A few other things to note:

- All instances of :class:`~ampel.abstract.AbsAlertFilter.AbsAlertFilter` have a ``self.logger`` property. You do not have to set one up yourself.
- ``AbsAlertFilter[PhotoAlert]`` indicates that the :meth:`~ampel.abstract.AbsAlertFilter.AbsAlertFilter.apply` method expects a :class:`~ampel.alert.PhotoAlert.PhotAlert`. Instances of :class:`~ampel.alert.PhotoAlert.PhotoAlert` have separate photopoints and upper limits. If you omit the parameter to :class:`~ampel.abstract.AbsAlertFilter.AbsAlertFilter` in your class definition, your :meth:`~ampel.abstract.AbsAlertFilter.AbsAlertFilter.apply` method will receive the base class, :class:`~ampel.alert.AmpelAlert.AmpelAlert`, instead. Instances of :class:`~ampel.alert.AmpelAlert.AmpelAlert` only have one collection of datapoints.
- The annotations are used to build a pydantic model that validates the configuration parameters defined for the instance in e.g. a channel definition. This means that you can use `Field() <https://pydantic-docs.helpmanual.io/usage/schema/#field-customisation>`_ to impose jsonschema-style constraints on the field value, for example requiring an integer to be positive, or a list to have a specified number of items.
- The ``#:`` comment marker indicates that the following is interface documentation, and should be included in autogenerated docs. Normal comments that start with just ``#`` are ignored.
- Field names should be lower camel-cased by convention.

Base classes also exist to automate the configuration of e.g. catalog matching services. For example, if you were previously setting up ``catsHTM`` matching
like this::
  
  from pydantic import BaseModel
  from ampel.base.abstract.AbsAlertFilter import AbsAlertFilter
  from ampel.contrib.hu import catshtm_server

  class GaiaVetoFilter(AbsAlertFilter):
  
      resources = ('catsHTM.default',)
  
      def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
          catshtm_uri = base_confg["catsHTM.default"]
          self.catshtm = catshtm_server.get_client(catshtm_uri)

you can inherit from :class:`~ampel.contrib.hu.base.CatsHTMUnit.CatsHTMUnit` and simplify to::
  
  from ampel.alert.PhotoAlert import PhotoAlert
  from ampel.abstract.AbsAlertFilter import AbsAlertFilter
  from ampel.contrib.hu.base.CatsHTMUnit import CatsHTMUnit

  class GaiaVetoFilter(CatsHTMUnit, AbsAlertFilter[PhotoAlert]):
      ...

:class:`~ampel.contrib.hu.base.ExtCatsUnit.ExtCatsUnit` is the equivalent for `extcats <https://github.com/MatteoGiomi/extcats>`_.

Filtering
*********

:class:`~ampel.alert.PhotoAlert.PhotoAlert` is mostly a drop-in replacement for the v0.6 :py:class:`AmpelAlert`. There are important differences, however:

- :meth:`~ampel.alert.PhotoAlert.PhotoAlert.get_values` uses native field names instead of the internal aliases from v0.6. Use ``jd`` instead of ``obs_date``, ``magpsf`` instead of ``mag``, etc.
- The third argument to :meth:`~ampel.alert.PhotoAlert.PhotoAlert.get_values` is now a string rather than a bool. Where you formerly used ``get_values(..., upper_limits=True)`` to get values from upper limits, use ``get_values(..., data="uls")``. To get both detections and upper limits, use ``get_values(..., data="all")``.

The return value of :meth:`AbsAlertFilter.apply <ampel.abstract.AbsAlertFilter.AbsAlertFilter.apply>` may now return a :class:`bool` or an :class:`int`.

- If you previously returned ``self.on_match_t2_units`` to accept an alert and trigger all configured T2s, return ``True`` instead.
- If you previously returned ``False`` or ``None`` to reject an alert, you may continue to do so. You may also return an integer "rejection code" between -255 and -1. You can define these codes however you like, and use them to efficiently query the properties of rejected alerts after the fact.
- If you previously returned a subset of ``self.on_match_t2_units`` depending on the exact properties of the alert, return a positive integer instead. This will be interpreted as id of the group of T2s to run.

T2 units
========

T2 units need to be :ref:`registered in your project's config <contribute-configuration-channels>`.

New base classes
****************

There are now 3 different kinds of T2 unit. If your T2 does something other than a light curve analysis, it may be a better fit for one of the new ones:

- :class:`~ampel.abstract.AbsLightCurveT2Unit.AbsLightCurveT2Unit` operates on entire light curves, and runs every time a new photopoint or upper limit is added to a transient. This is equivalent to the old :py:class:`AbsT2Unit`, but can be configured to operate on all photopoints, or on detections only.
- :class:`~ampel.abstract.AbsPointT2Unit.AbsPointT2Unit` operates on single data points. It can be configured to run on a subset of photopoints, e.g. to run catalog matching on only the first detection.
- :class:`~ampel.abstract.AbsStockT2Unit.AbsStockT2Unit` operates on the stock (transient) record itself. This can be used to perform some action when the transient is added to a channel.

There are also "tied" variants of these, such as :class:`~ampel.abstract.AbsTiedLightCurveT2Unit.AbsTiedLightCurveT2Unit`, that can be used to build a directed acyclic graph of T2s. In other words, these T2s depend on the output of other T2s.

.. NB: while base classes for tied T2s exist, T2Process does not actually know how to defer dependent T2s until their dependencies have been processed.

T2 unit configuration
*********************

Like T0 units, T2 units declare their configurations using annotations. See :ref:`legacy-t0-configuration`.

:py:meth:`run`
**************

T2 units now have a single configuration, so the :meth:`~ampel.abstract.AbsLightCurveT2Unit.AbsLightCurveT2Unit.run` method no longer takes a ``run_config`` argument. If your :py:meth:`run` method previously looked like this [contrived] example::
  
  def run(self, light_curve, run_config):
      count = len(light_curve.get_values("jd", upper_limits=False))
      if run_config["include_upper_limits"]:
          count += len(light_curve.get_values("jd", upper_limits=True))
      return {"length": count}

it should be replaced with::
  
  def run(self, lightcurve: LightCurve) -> T2UnitResult:
      count = len(light_curve.get_values("jd", of_upper_limits=False))
      if run_config["include_upper_limits"]:
          count += len(light_curve.get_values("jd", of_upper_limits=True))
      return {"length": count}

The `PEP 484 annotations <https://www.python.org/dev/peps/pep-0484/>`_ in the method signature are optional but highly encouraged. If these type hints are present, static type checkers like mypy_ will be able to spot mistakes like returning the wrong type, calling a method that does not exist or with the wrong arguments, etc.

T3 units
========

T2 units need to be :ref:`registered in your project's config <contribute-configuration-channels>`.

T3 unit configuration
*********************

See :ref:`legacy-t0-configuration`.

If your T3 authenticates with an external service like Slack or DropBox using a secret token, you should *not* check this token into your repository. Slack in particular scans all commits to public GitHub repositories and revokes any of its tokens it finds there. Instead, you can use the special :class:`~ampel.model.Secret.Secret` type hint to indicate that the value should be looked up in a separate secret store. For example, if you previously had::
  
  from pydantic import BaseModel, BaseConfig
  from ampel.base.abstract.AbsT3Unit import AbsT3Unit
  
  class LooseLipsSinkShips(AbsT3Unit):
      class RunConfig(BaseModel):
          slack_token: str =  "xoxb-216058338329-819573451732-Rjxt1zb9WpjhVZ6H6Y3ZUuHo"
      def __init__(self, logger, base_config=None, run_config=None, global_info=None):
          self.run_config = self.RunConfig() if run_config is None else run_config
      def add(self, views):
          token = self.run_config["slack_token"]
          ...

you should have this::
  
  from typing import Dict, Tuple
  
  from ampel.abstract.AbsT3Unit import AbsT3Unit
  from ampel.model.Secret import Secret
  from ampel.struct.JournalExtra import JournalExtra
  from ampel.type import StockId
  from ampel.view.TransientView import TransientView
  
  class Skrytnost(AbsPhotoT3Unit):
  
      slack_token: Secret[str] = {"key": "my-slack-token"}  # type: ignore[assignment]
  
      def add(self, transients: Tuple[TransientView, ...]) -> Dict[StockId, JournalExtra]:
          token = self.slack_token.get()
          ...

Again, all type annotations in method signatures (and the associated imports) are optional, but encouraged. The default value of ``slack_token`` tells Ampel to look up the value under the name "my-slack-token" in its secret store. Your T3 instance will be configured with an object whose :meth:`~ampel.model.Secret.Secret.get` method returns the value (of the type indicated in ``[]``, or :class:`str` if unspecified). This mechanism allows you to specify which token you want by default as a symbolic name rather than a value. The trailing comment instructs mypy_ to not complain about the assignment.

:py:meth:`~ampel.abstract.AbsT3Unit.AbsT3Unit.add`
**************************************************

- return a ``Dict[StockId,JournalExtra]`` instead of a list of ``JournalUpdate``. For example, if you were previously doing something like this::
  
    jupdates = []
    for tran_view in views:
        jcontent = {'t3unit': self.name, 'reactDict': do_something(tran_view), 'success':success}
        jupdates.append(JournalUpdate(tran_id=tran_view.tran_id) ext=self.run_config.ext_journal, content=jcontent)
    return jupdates

  you can replace that with::
  
    jupdates = {}
    for tran_view in views:
        jupdates[tran_view.id] = JournalExtra(extra={'reactDict': do_something(tran_view), 'success':success})
    return jupdates
- For current ZTF transients, the ZTF name is the first element of the stock name, e.g.::
    
    transient_view.stock["name"][0]
  
  To be extra-pendantic (and pass all mypy_ checks), use::
    
    assert view.stock and view.stock["name"] is not None
    name = next(
        n for n in view.stock["name"] if isinstance(n, str) and n.startswith("ZTF")
    )


.. _mypy: https://mypy.readthedocs.io/en/stable/
.. _YAML: https://en.wikipedia.org/wiki/YAML
.. _JSON: https://en.wikipedia.org/wiki/JSON
.. _yq: https://mikefarah.gitbook.io/yq/
