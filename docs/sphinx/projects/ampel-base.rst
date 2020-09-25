
Ampel-interface
===============

Ampel-interface provides base classes for implementing Ampel processing units,
as well as the data classes that are provided as input to those units by the
core.

Processing units
----------------

.. autoclass:: ampel.abstract.AbsAlertFilter.AbsAlertFilter
  :show-inheritance:
  :members:

.. autoclass:: ampel.abstract.AbsPointT2Unit.AbsPointT2Unit
  :show-inheritance:
  :members:

.. autoclass:: ampel.abstract.AbsStockT2Unit.AbsStockT2Unit
  :show-inheritance:
  :members:

.. autoclass:: ampel.abstract.AbsTiedStateT2Unit.Dependency
  :show-inheritance:
  :members:

.. autoclass:: ampel.abstract.AbsTiedCustomStateT2Unit.AbsTiedCustomStateT2Unit
  :show-inheritance:
  :members:

.. autoclass:: ampel.abstract.AbsT3Unit.AbsT3Unit
  :show-inheritance:
  :members:

Base classes for processing units
---------------------------------

.. autoclass:: ampel.base.DataUnit.DataUnit
  :members:

Context and configuration
-------------------------

.. autoclass:: ampel.config.AmpelConfig.AmpelConfig
  :members:

Data classes
------------

.. autoclass:: ampel.content.StockRecord.StockRecord
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.DataPoint.DataPoint
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.Compound.Compound
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.Compound.CompoundElement
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.T2Record.T2Record
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.T2SubRecord.T2SubRecord
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.LogRecord.LogRecord
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.content.JournalRecord.JournalRecord
  :members:
  :undoc-members:
  :private-members:

.. autoclass:: ampel.struct.JournalExtra.JournalExtra
  :members:
  :undoc-members:

.. autoclass:: ampel.view.SnapView.SnapView
  :members:

.. autoclass:: ampel.view.ReadOnlyDict.ReadOnlyDict
  :members:
  :private-members:

Data models
-----------

These models are used to parse and validate the configuration of the Ampel system.

.. autoclass:: ampel.model.Secret.Secret
  :show-inheritance:
  :members:

.. autoclass:: ampel.model.UnitModel.UnitModel
  :members: