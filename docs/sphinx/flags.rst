
Ampel flags
===========

Ampel flags are twofold.

- On the Python side, those are implemented using Flag enumerations (enum.Flag).
  As a consequence, flags are quite convenient to manipulate. Since python has no
  integer size limitation, Flag enumerations can grow above 64 elements.

- On the Mongo DB side, the situation is more complicated. Following restrictions
  apply:
  
  * The `$bit` update operator is not usable for numbers no longer
    representable as `int64_t`. (https://jira.mongodb.org/browse/SERVER-15680).
    It means that if the number of flag values of a given enum exceeds 63,
    updates have to be performed using the $set operator which can lead to race
    conditions.

  * Bitwise query operators (for example `$bitsAnySet`) are not available in the
    aggregation framework (https://jira.mongodb.org/browse/SERVER-25823).

  * There is no index support for Bitwise query operations
    (https://jira.mongodb.org/browse/SERVER-24749).

That's why ampel converts Flag enumerations into an array of flag index
positions before saving flags into the mongo database. A (bijective) conversion
from Flag enumeration values (powers of two) into index position is used to
avoid having to deal with BinData encoding of numbers no longer representable
as `int64_t`.

Example::
  
  In [0]: from ampel.flags.PhotoPointFlags import PhotoPointFlags

  In [1]: PhotoPointFlags??
  Out[1]: 
  class PhotoPointFlags(Flag):

          DATE_JD = 1
          FLUX_MAG_VEGA = 2
          FLUX_MAG_AB = 4
          INST_ZTF  = 8
          ZTF_G = 16
          ...

  In [2]: f = PhotoPointFlags.FLUX_MAG_AB | PhotoPointFlags.ZTF_G

  In [3]: f
  Out[3]: <PhotoPointFlags.INST_ZTF|FLUX_MAG_AB: 12>

  In [4]: from ampel.pipeline.common.db.FlagUtils import FlagUtils

  In [5]: FlagUtils.enumflag_to_mongoflag(f)
  Out[5]: [3, 4]

  In [6]: FlagUtils.mongoflag_to_enumflag([3,5], PhotoPointFlags)
  Out[6]: <PhotoPointFlags.ZTF_G|FLUX_MAG_AB: 20>
