
Ampel-base
==========

Ampel-base provides base classes for implementing Ampel processing units, as
well as the data classes that are provided as input to those units by the core.

Ampel processing units
----------------------

.. autoclass:: ampel.base.abstract.AbsAlertFilter.AbsAlertFilter
  :members:
  
  .. automethod:: __init__

.. autoclass:: ampel.base.abstract.AbsT2Unit.AbsT2Unit
  :members:
  
  .. automethod:: __init__

Ampel data classes
------------------

.. autoclass:: ampel.base.AmpelAlert.AmpelAlert
  :members:

.. autoclass:: ampel.base.LightCurve.LightCurve
  :members:

.. autoclass:: ampel.base.ScienceRecord.ScienceRecord
  :members:

.. autoclass:: ampel.base.TransientView.TransientView
  :members:
