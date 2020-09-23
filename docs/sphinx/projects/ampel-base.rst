
Ampel-interface
===============

Ampel-interface provides base classes for implementing Ampel processing units,
as well as the data classes that are provided as input to those units by the
core.

Processing units
----------------

.. autoclass:: ampel.abstract.AbsAlertFilter.AbsAlertFilter
  :members:

.. autoclass:: ampel.abstract.AbsPointT2Unit.AbsPointT2Unit
  :members:

.. autoclass:: ampel.abstract.AbsStockT2Unit.AbsStockT2Unit
  :members:

.. autoclass:: ampel.abstract.AbsT3Unit.AbsT3Unit
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

Data models
-----------

These models are used to parse and validate the configuration of the Ampel system.

.. autoclass:: ampel.model.Secret.Secret
  :show-inheritance:
  :members:
