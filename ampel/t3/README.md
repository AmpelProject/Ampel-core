# Ampel third and last execution layer

The default top level implementation provided by _ampel-core_
is the class _T3Processor_, which is capable of spawning and executing
T3 processes according to the provided configuration.
The execution typically follows a 5-step process governed by
the following top-level sections from the configuration:

- session
- select
- load
- complement
- stage

Foreword: within ampel, a unique ID called 'stock ID' is assigned to all documents
associated with a given entity across the four underlying collections.


## Session (optional)
Package: _ampel.t3.session_\
Governing abstract class: _AbsSessionInfo_\
Known implementing class: _T3SessionLastRunTime_, _T3SessionAlertsNumber_

Allows to generate session information that will be provided to T3 units.
Note that the information gathered in this step is not associated
with individual ampel elements (each identified by a unique 'stock ID').


## Select

Package: _ampel.t3.select_\
Governing abstract class: _AbsT3Selector_\
Provided default implementation: _T3StockSelector_

Allows to select which elements should be provided to T3 units.
The default implementation _T3StockSelector_ selects stock IDs based on
criteria targeting the internal collection 'stock'.
Note that other implementations are possible, in particular implementations
based on the information from the internal collection 't2'.
The stock IDs are extracted from the returned documents and using in the next stage.


## Load

Package: _ampel.t3.load_\
Governing abstract class: _AbsT3DataLoader_\
Known implementing classes: _T3DataLoader_, _T3LatestStateDataLoader_

Regulates which documents to load for each ampel ID selected by the previous stage.
The loaded documents are then included into _AmpelBuffer_ instances which are passed to the next stages.
Note that all current implementations rely internally on the backend class _DataLoader_.


## Complement (optional)

Package: _ampel.t3.complement_\
Governing abstract class: _AbsT3Complementer_\
Known implementing class: _T3CompExtJournal_, _T3CompSEDMSpec_

Allows to include additional information to the loaded _AmpelBuffer_ instances.
Most implementations are expected to update the _AmpelBuffer_ attribute 'extra'
which was specifically added for this purpose.
Other implementations, such as _T3CompExtJournal_ update the content
of the AMPEL core documents themselves.
Since the information only lives in the process memory
(not saved into the DB, only included into the _views_),
there is no hard limitation wrt to data type / serialization property / size


## Stage

Package: _ampel.t3.stage_\
Governing abstract class: _AbsT3Stager_\
Provided implementations: _T3SimpleStager_, _T3ProjectingStager_, _T3ChannelStager_, _T3DisitributingStager_, _T3SequentialStager_\

Stagers provide t3 units with _AmpelBuffer_ instances.

_T3ProjectingStager_ follows a 3-step process governed by the following sections from the configuration:

- filter
- project
- execute

### Filter (optional)

Package: _ampel.t3.stage.filter_\
Governing abstract class: _AbsT3Filter_\
Provided default implementation: _T3AmpelBufferFilter_

Foreword: this setting applies only in case the underlying T3 process runs multiple units.
The optional setting 'filter' allows to define selection criteria for the loaded _AmpelBuffer_ instances.
Only matching instances are passed to the next stage.

Example: say you want to select all new entities that were associated with your channel the last 24 hours,
and post information about them to slack. Furthermore, say your entities come in blue or red and you'd like
to post "blue entities" into the slack channel "#blue" and "red entities" into the "#red" channel.
1) Either you define two separate processes with distinct top level setting _select_ (described above),
which will then be scheduled separately. You do not need the setting _filter_ in this case.
2) Or you can create a single process selecting both red and blue entities and define two "run blocks"
under the top-level setting _run_ (described above). The first run-block sub-selects blue entities
and posts them to "#blue" and the second one sub-selects red entities and posts then to "#red".


### Project (optional)

Package: _ampel.t3.stage.project_\
Governing abstract class: _AbsT3Projector_\
Provided implementations: _T3BaseProjector_, _T3ChannelProjector_, _T3MultiChannelProjector_

The process potentially:
- strips out 'channel' attributes
- removes journal entries not associated with configured channels
- removes t2 results not associated with configured channels

Notes:
- this stage, although modular as most of ampel is, is not expected to be customized in most cases.
- the template associated with channel definitions usually automatically configure this stage


### Execute

Package: _ampel.t3.stage_\
Governing abstract class: _AbsT3UnitRunner_\
Provided implementations: _T3UnitRunner_, _T3DynChanViewUnitRunner_

Last step during which instantiated T3 units are provided with _views_ (containing pseudo-immutable structures)
generated from  _AmpelBuffer_ instances.

T3 unit instances also have access to the session information loaded during the previous stages.