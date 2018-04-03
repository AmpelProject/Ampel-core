## Functions for pushing/pulling the Marshall (scanning page)
Tested on python 2.7
```
import read_scanning as rs

prog_name = 'ZTF Science Validation'
inst = rs.marshal_scanning(prog_name, start_date = '2018-04-03', end_date = '2018-04-03')
sources = inst.list_sources()
progid = inst.program
# To save a source to a science program
rs.save_source(candid, progid)
```
