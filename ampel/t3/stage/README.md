# Stagers

They receive an `AmpelBuffer` generator as input and provide t3 units with a `SnapView` generator and a `T3Store` instance.

## T3SimpleStager
To be used in combination with mongo views (no python-based projection done).

## T3DistributiveStager
Allows to execute a given unit multiple times in different parallel threads (with the same config).\
Each unit processes a subset of the initial ampel buffer stream.\
Example: consider 2 threads and the buffers `ABCD`: thread1 receives `A`, thread2 receives `B`, thread1 receive `C`, thread2 receives `D`.\
This shall allow better performance when used in combination with T3 units that are slowed down by IO based operations (such as network requests to external services).\
Note that no performance gain will be obtained if the processing is CPU limited.

## T3AdaptativeStager
For each channel found in the stock documents loaded by the previous T3 stages,
spawns a dedicated `T3ProjectingStager` instance configured to filter and
project elements wrt this channel and execute the associated T3 units.

## T3SequentialStager
Calls t3 units methods sequentially while:
1) The source `AmpelBuffer` generator is fully consumed and buffers are converted to views which are stored in memory (all of them).\
Each T3 units is provided with a new generator based on those views.
2) Results from upstream t3 units are made available to downstream units through `T3Store.views` which is updated after each unit execution.

## T3ChannelStager
Configures an underlying `T3ProjectingStager` instance to filter and project `AmpelBuffer` instances with respect to a single channel.\
Essentially a shortcut class since the same functionality can be obtained using a properly configured T3ProjectingStager instance.

## T3ProjectingStager
A stager with tuneable python-based projections.

_T3ProjectingStager_ follows a 3-step process governed by the following sections from the configuration:

- filter
- project
- execute

### Filter (optional)

Package: _ampel.t3.stage.filter_\
Governing abstract class: `AbsT3Filter`\
Provided default implementation: `T3AmpelBufferFilter`

Foreword: this setting applies only in case the underlying T3 process runs multiple units.
The optional setting 'filter' allows to define selection criteria for the loaded `AmpelBuffer` instances.
Only matching instances are passed to the next stage.

Example: say you want to select all new entities that were associated with your channel the last 24 hours,
and post information about them to slack. Furthermore, say your entities come in blue or red and you'd like
to post "blue entities" into the slack channel "#blue" and "red entities" into the "#red" channel.
1) Either you define two separate processes with distinct top level setting `select` (described above),
which will then be scheduled separately. You do not need the setting `filter` in this case.
2) Or you can create a single process selecting both red and blue entities and define two "run blocks"
under the top-level setting `run` (described above). The first run-block sub-selects blue entities
and posts them to "#blue" and the second one sub-selects red entities and posts then to "#red".


### Project (optional)

Package: _ampel.t3.stage.project_\
Governing abstract class: `AbsT3Projector`\
Provided implementations: `T3BaseProjector`, `T3ChannelProjector`, `T3MultiChannelProjector`

The process potentially:
- strips out 'channel' attributes
- removes journal entries not associated with configured channels
- removes t2 results not associated with configured channels

Notes:
- this stage, although modular as most of ampel is, is not expected to be customized in most cases.
- the template associated with channel definitions usually automatically configure this stage.


### Execute

Instantiated T3 units are provided with _views_ (and a `T3Store` instance)
generated from  `AmpelBuffer` instances.
