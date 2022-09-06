
Ampel-core
==========

The actual implementation of the Ampel framework

Processing units
----------------

.. autoclass:: ampel.t3.session.T3SessionLastRunTime.T3SessionLastRunTime
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.session.T3SessionAlertsNumber.T3SessionAlertsNumber
  :members:
  :show-inheritance:

Base classes for processing units
---------------------------------

.. autoclass:: ampel.core.ContextUnit.ContextUnit
  :members:

.. autoclass:: ampel.abstract.AbsEventUnit.AbsEventUnit
  :members:

Processor units
---------------

.. autoclass:: ampel.t2.T2Processor.T2Processor
  :members:
  :exclude-members: load_input_docs, sig_exit, create_beacon, view_from_record, build_tied_t2_query, run_t2_unit, push_t2_update
  :show-inheritance:

.. autoclass:: ampel.t3.T3Processor.T3Processor
  :members:
  :show-inheritance:

T3 machinery
------------

.. autoclass:: ampel.model.t3.T3Directive.T3Directive
  :members:

.. autoclass:: ampel.t3.context.AbsT3RunContextAppender.AbsT3RunContextAppender
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.select.AbsT3Selector.AbsT3Selector
  :members:

.. autoclass:: ampel.t3.select.T3StockSelector.T3StockSelector
  :members:

.. autoclass:: ampel.t3.select.T3FilteringStockSelector.T3FilteringStockSelector
  :members:

.. autoclass:: ampel.t3.load.AbsT3Loader.AbsT3Loader
  :members:

.. autoclass:: ampel.t3.load.T3SimpleDataLoader.T3SimpleDataLoader
  :members:

.. autoclass:: ampel.t3.load.T3LatestStateDataLoader.T3LatestStateDataLoader
  :members:

.. autoclass:: ampel.t3.complement.AbsT3DataAppender.AbsT3DataAppender
  :members:

.. autoclass:: ampel.t3.complement.T3ExtJournalAppender.T3ExtJournalAppender
  :members:

.. autoclass:: ampel.t3.run.AbsT3UnitRunner.AbsT3UnitRunner
  :members:

.. autoclass:: ampel.t3.run.T3UnitRunner.T3UnitRunner
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.run.T3DynamicUnitRunner.T3DynamicUnitRunner
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.run.filter.AbsT3Filter.AbsT3Filter
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.run.filter.T3AmpelBufferFilter.T3AmpelBufferFilter
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.run.project.AbsT3Projector.AbsT3Projector
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.run.project.T3BaseProjector.T3BaseProjector
  :members:
  :show-inheritance:

.. autoclass:: ampel.t3.run.project.T3ChannelProjector.T3ChannelProjector
  :members:
  :show-inheritance:


Context and configuration
-------------------------

.. autoclass:: ampel.core.AmpelContext.AmpelContext
  :members:

.. autoclass:: ampel.core.UnitLoader.UnitLoader
  :members:

.. autoclass:: ampel.dev.DictSecretProvider.DictSecretProvider
  :members:

Data classes
------------

.. autoclass:: ampel.core.AmpelBuffer.AmpelBuffer
  :members:
  :undoc-members:

Models
------

.. autoclass:: ampel.model.operator.AnyOf.AnyOf
  :members:

.. autoclass:: ampel.model.operator.AllOf.AllOf
  :members:

.. autoclass:: ampel.model.operator.OneOf.OneOf
  :members:

.. autoclass:: ampel.model.time.TimeConstraintModel.TimeConstraintModel
  :members:
  :undoc-members:

.. autoclass:: ampel.model.time.TimeDeltaModel.TimeDeltaModel
  :members:
  :undoc-members:

.. autoclass:: ampel.model.time.TimeLastRunModel.TimeLastRunModel
  :members:
  :undoc-members:

.. autoclass:: ampel.model.time.TimeStringModel.TimeStringModel
  :members:
  :undoc-members:

.. autoclass:: ampel.model.time.UnixTimeModel.UnixTimeModel
  :members:
  :undoc-members:

.. autoclass:: ampel.model.t3.LoaderDirective.LoaderDirective
  :members:
  :exclude-members: dict

.. autoclass:: ampel.model.t3.T2FilterModel.T2FilterModel
  :members:

.. autoclass:: ampel.model.ingest.IngestDirective.IngestDirective
  :members:

.. autoclass:: ampel.model.ingest.T0AddModel.T0AddModel
  :members:
  :show-inheritance:

.. autoclass:: ampel.model.ingest.T1CombineModel.T1CombineModel
  :members:
  :show-inheritance:

.. autoclass:: ampel.model.ingest.T2ComputeModel.T2ComputeModel
  :members:
  :show-inheritance:

.. autoclass:: ampel.model.ingest.T2IngestModel.T2IngestModel
  :members:
  :show-inheritance:


Templates
---------

.. autoclass:: ampel.model.template.AbsLegacyChannelTemplate.AbsLegacyChannelTemplate
  :members:
  :exclude-members: craft_t0_process, get_t2_units
  :show-inheritance:

.. autoclass:: ampel.model.template.AbsLegacyChannelTemplate.T2UnitModel
  :members:
  :show-inheritance:

.. autoclass:: ampel.model.template.PeriodicSummaryT3.FilterModel
  :members:

.. autoclass:: ampel.model.template.PeriodicSummaryT3.PeriodicSummaryT3
  :members:
  :exclude-members: get_channel_tag
  :show-inheritance:

Odds and ends
-------------

.. autoclass:: ampel.log.AmpelLogger.AmpelLogger
  :members:

.. autoclass:: ampel.core.AmpelRegister.AmpelRegister
  :members:
