Ampel-photometry
================

Classes for photometric data

Processing units
----------------

.. autoclass:: ampel.abstract.AbsLightCurveT2Unit.AbsLightCurveT2Unit
  :members:
  
  .. automethod:: run

.. autoclass:: ampel.abstract.AbsTiedLightCurveT2Unit.AbsTiedLightCurveT2Unit
  :show-inheritance:
  :members:

.. autoclass:: ampel.abstract.AbsPhotoT3Unit.AbsPhotoT3Unit
  :members:

Data classes
------------

.. autoclass:: ampel.alert.PhotoAlert.PhotoAlert
  :members:

.. autoclass:: ampel.view.LightCurve.LightCurve
  :members:

.. autoclass:: ampel.view.TransientView.TransientView
  :members:
  :inherited-members:
  :undoc-members:
  :show-inheritance:

T1 machinery
------------

.. autoclass:: ampel.ingest.compile.PhotoT2Compiler.PhotoT2Compiler
  :members: