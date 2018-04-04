## Functions for pushing/pulling the Marshall (scanning page)
Tested on python 2.7
First, creat a config file called ~/.ptfconfig.cfg with the following content.
```
[Marshal]
user = your_username_on_marshal
passw = your_password
maxage = 1
```
```
from marshal_functions import Sergeant

prog_name = 'ZTF Science Validation'
inst = Sergeant.marshal_scanning(prog_name, start_date = '2018-04-03', end_date = '2018-04-03')
sources = inst.list_scan_sources()
progid = inst.program
# To save a source to a science program
rs.save_source(candid, progid)
```
Comment on view source page.
```
rs.annotate("Scheduled for DCT on 4/3", 'ZTF18aagteoy', comment_type="info")
```
