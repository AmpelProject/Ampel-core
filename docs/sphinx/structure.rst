Structure
---------

Tiers
=====

Data processing is divided into 4 tiers.

.. _structure-t0:

Tier 0: Add
###########

Ingest (or reject) incoming :class:`DataPoints <ampel.content.DataPoint.DataPoint>`.

.. _structure-t1:

Tier 1: Combine
###############

Creates :class:`Compounds <ampel.content.Compound.Compound>` documents, sometimes referred to as 'states', based on collections of :class:`DataPoints <ampel.content.DataPoint.DataPoint>`.

.. _structure-t2:

Tier 2: Compute
###############

Compute derived quantities from newly added :class:`StockRecords <ampel.content.StockRecord.StockRecord>`, :class:`DataPoints <ampel.content.DataPoint.DataPoint>`, and :class:`Compounds <ampel.content.Compound.Compound>`.

.. _structure-t3:

Tier 3: React
#############

Perform action based on collections of Ampel objects.

The default top level implementation provided by ``ampel-core``
is the class :class:`~ampel.t3.T3Processor.T3Processor`, which is capable of spawning and executing
T3 processes according to the provided configuration.
The execution typically follows a 5-step process specified by the following fields of :class:`~ampel.model.t3.T3Directive.T3Directive`:

* :ref:`t3-directive-context`
* :ref:`t3-directive-select`
* :ref:`t3-directive-load`
* :ref:`t3-directive-complement`
* :ref:`t3-directive-run`
   * :ref:`t3-directive-run-filter`
   * :ref:`t3-directive-run-project`
   * :ref:`t3-directive-run-execute`

.. note::
  Within Ampel, a unique ID called 'stock ID' is assigned to all documents
  associated with a given entity across T1, T2 and T3 (and possibly T0 if already known at this point).

.. _t3-directive-context:

context
^^^^^^^

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.context`
Governing abstract class                 :class:`~ampel.t3.context.AbsT3RunContextAppender.AbsT3RunContextAppender`
Known implementations                    :class:`~ampel.t3.context.T3AddLastRunTime.T3AddLastRunTime`,
                                         :class:`~ampel.t3.context.T3AddAlertsNumber.T3AddAlertsNumber`
======================================== =========================

Allows to generate global information that will be provided to T3 units.
Note that the information gathered in this stage is not associated
with individual ampel elements (each identified by a unique 'stock ID').

.. _t3-directive-select:

select
^^^^^^

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.select`
Governing abstract class                 :class:`~ampel.t3.select.AbsT3Selector.AbsT3Selector`
Known implementations                    :class:`~ampel.t3.select.T3StockSelector.T3StockSelector`,
                                         :class:`~ampel.t3.select.T3FilteringStockSelector.T3FilteringStockSelector`
======================================== =========================

Allows to select which elements should be provided to T3 units.
The default implementation :class:`~ampel.t3.select.T3StockSelector.T3StockSelector` selects stock IDs based on
criteria targeting the internal collection 'stock'.
Note that other implementations are possible, in particular implementations
based on the information from the internal collection 't2'.
The returned sequence of stock IDs is passed to the next stage.

.. _t3-directive-load:

load
^^^^

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.load`
Governing abstract class                 :class:`~ampel.t3.load.AbsT3Loader.AbsT3Loader`
Known implementations                    :class:`~ampel.t3.load.T3SimpleDataLoader.T3SimpleDataLoader`,
                                         :class:`~ampel.t3.load.T3LatestStateDataLoader.T3LatestStateDataLoader`
======================================== =========================

Regulates which documents to load for each ampel ID selected by the previous stage.
The loaded documents are then included into :class:`~ampel.core.AmpelBuffer.AmpelBuffer` instances which are passed to the next stages.
Note that all current implementations rely internally on the backend class :class:`~ampel.db.DBContentLoader.DBContentLoader`.

.. _t3-directive-complement:

complement
^^^^^^^^^^

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.complement`
Governing abstract class                 :class:`~ampel.t3.complement.AbsT3DataAppender.AbsT3DataAppender`
Known implementations                    :class:`~ampel.t3.complement.T3ExtJournalAppender.T3ExtJournalAppender`,
                                         :class:`~ampel.t3.complement.SEDMSpectrumAppender.SEDMSpectrumAppender`,
                                         :class:`~ampel.contrib.hu.t3.complement.TNSNames.TNSNames`,
                                         :class:`~ampel.ztf.t3.complement.ZTFCutoutImages.ZTFCutoutImages`
======================================== =========================

Allows to include additional information to the loaded :class:`~ampel.core.AmpelBuffer.AmpelBuffer` instances.
Most implementations are expected to update :class:`AmpelBuffer.extra <ampel.core.AmpelBuffer.AmpelBuffer>`
which was specifically added for this purpose.
Other implementations, such as :class:`~ampel.t3.complement.T3ExtJournalAppender.T3ExtJournalAppender` update the content
of the AMPEL core documents themselve.
Since the information only lives in the process memory
(not saved into the DB, only included into the *views*),
there is no hard limitation wrt to data type / serialization property / size

.. _t3-directive-run:

run
^^^

The last stage executes t3 units according the provided configuration
and using the various information loaded from previous stages.

The default implementation :class:`~ampel.t3.run.T3UnitRunner.T3UnitRunner` provided by ``ampel-core``
follows a 3-step process governed by the following sections from the configuration:

- filter
- project
- execute

.. _t3-directive-run-filter:

filter
""""""

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.run.filter`
Governing abstract class                 :class:`~ampel.t3.run.filter.AbsT3Filter.AbsT3Filter`
Known implementations                    :class:`~ampel.t3.run.filter.T3AmpelBufferFilter.T3AmpelBufferFilter`
======================================== =========================

.. note:: This setting applies only in case the underlying T3 process runs multiple units.

The optional setting 'filter' allows to define selection critera for the loaded :class:`~ampel.core.AmpelBuffer.AmpelBuffer` instances.
Only matching instances are passed to the next stage.

Example: say you want to select all new entities that were associated with your channel the last 24 hours,
and post information about them to slack. Furthermore, say your entities come in blue or red and you'd like
to post "blue entities" into the slack channel "#blue" and "red entities" into the "#red" channel.
1) Either you define two separate processes with distinct top level setting :ref:`t3-directive-select`,
which will then be scheduled seperately. You do not need the setting :ref:`t3-directive-run-filter` in this case.
2) Or you can create a single process selecting both red and blue entities and define two "run blocks"
under the top-level setting :ref:`t3-directive-run`. The first run-block sub-selects blue entities
and posts them to "#blue" and the second one sub-selects red entities and posts then to "#red".

.. _t3-directive-run-project:

project
"""""""

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.run.project`
Governing abstract class                 :class:`~ampel.t3.run.project.AbsT3Projector.AbsT3Projector`
Known implementations                    :class:`~ampel.t3.run.project.T3BaseProjector.T3BaseProjector`,
                                         :class:`~ampel.t3.run.project.T3ChannelProjector.T3ChannelProjector`
======================================== =========================

The process potentially:

- strips out 'channel' attributes
- removes journal entries not associated with configured channels
- removes t2 results not associated with configured channels

.. note:: this stage, although modular as most of Ampel is, is not expected to be customized in most cases.
.. note:: the template associated with channel definitions usually automatically configure this stage

.. _t3-directive-run-execute:

execute
"""""""

======================================== =========================

======================================== =========================
Package                                  :mod:`ampel.t3.run`
Governing abstract class                 :class:`~ampel.t3.run.AbsT3UnitRunner.AbsT3UnitRunner`
Known implementations                    :class:`~ampel.t3.run.T3UnitRunner.T3UnitRunner`,
                                         :class:`~ampel.t3.run.T3DynamicUnitRunner.T3DynamicUnitRunner`
======================================== =========================

Last stage during which T3 units are instantiated and :class:`~ampel.core.AmpelBuffer.AmpelBuffer` instances
converted into *views* (e.g. :class:`~ampel.view.SnapView.SnapView` and subclasses, containing pseudo-immutable structures).

T3 unit instances (i.e. instances of :class:`~ampel.abstract.AbsT3Unit.AbsT3Unit`) are provided both with *views* and
`global information <t3-directive-context>`_ loaded during the previous stages.
They have also the possibility to customize the journal entry
created each time the underlying process is run.
