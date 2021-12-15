# Ampel third execution layer

The default top level implementation provided by _ampel-core_
is _T3Processor_, which is capable of spawning and executing
T3 processes according to configuration.
The execution process is governed by the top-level config sections _include_ and _execute_

Foreword: within ampel, a unique ID called 'stock ID' is assigned to all documents
associated with a given entity across the four underlying collections.

## Include (optional)
Defines what information should be made available in the _T3Store_ instance provided to subsequent ampel units.\
Note that the information gathered in this step is not associated
with individual ampel elements (each identified by a unique 'stock ID').
The include section contains two sub-sections: _docs_ and _session_.

### Docs (optional)
Package: _ampel.t3.include_\
Config type: `UnitModel`\
Governing abstract class: Not implemented yet

The associated unit should return an `Iterable[T3Document]`.\
Each document will be cast into `T3DocView` and added to `T3Store.views`

### Session (optional)
Package: _ampel.t3.include.session_\
Config Type: `Union[None, UnitModel, Sequence[UnitModel]]`\
Governing abstract class: `AbsT3Supplier`\
Known implementing class: `T3SessionLastRunTime`, `T3SessionAlertsNumber`

Fetch/generate session information that will be provided to T3 units via `T3Store.session`.

## Execute

Contains a list of directives.\
A commonly used directive is `T3ReviewDirective` which allows to execute `AbsT3ReviewUnit` instances

### T3ReviewDirective

#### Supply

##### Select

Package: _ampel.t3.select_\
Governing abstract class: `AbsT3Selector`\
Provided default implementation: `T3StockSelector`

Allows to select which elements should be provided to T3 units.
The default implementation `T3StockSelector` selects stock IDs based on
criteria targeting the internal collection 'stock'.
Note that other implementations are possible, in particular implementations
based on the information from the internal collection 't2'.
The stock IDs are extracted from the returned documents and using in the next stage.


##### Load

Package: _ampel.t3.load_\
Governing abstract class: `AbsT3DataLoader`\
Known implementing classes: `T3DataLoader`, `T3LatestStateDataLoader`

Regulates which documents to load for each ampel ID selected by the previous stage.
The loaded documents are then included into _AmpelBuffer_ instances which are passed to the next stages.
Note that all current implementations rely internally on the backend class _DataLoader_.


##### Complement (optional)

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


#### Stage

Package: _ampel.t3.stage_\
Governing abstract class: `AbsT3Stager`\
Provided implementations: `T3SimpleStager`, `T3AggregatingStager`, `T3AdaptativeStager`, `T3ChannelStager`, `T3DistributingStager`, `T3SequentialStager`, `T3ProjectingStager`\

Stagers provide t3 units with _SnapView_ instances.

### Other directives

...
