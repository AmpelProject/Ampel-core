## Functions for pushing/pulling the Marshall (scanning page)
Tested on python 2.7
First, creat a config file called ~/.ptfconfig.cfg with the following content.
```
[Marshal]
user = your_username_on_marshal
passw = your_password
```
```
import marshal_functions

prog_name = 'ZTF Science Validation'
ser = marshal_functions.Sergeant(prog_name, start_date = '2018-04-03', end_date = '2018-04-03')

# Get list of source on the scan page
scan_sources = ser.list_scan_sources(hardlimit=200)
progid = ser.program

# Save a source to a science program
marshal_functions.save_source(candid, progid)


# Get list of sources saved sources (ie, the Report page)
saved_scources = ser.list_saved_sources()
```

View current comments (note, these do include the auto annotations)
```
comment_list = marshal_functions.get_comments('ZTF18aabtxvd')
```


A new comment on view source page.
```
marshal_functions.comment("dummy", 'ZTF17aacscou', comment_type="info")
```

Replace comment
```
marshal_functions.comment("dummy", 'ZTF17aacscou', comment_type="info")
comment_list = marshal_functions.get_comments('ZTF17aacscou')
comment_id = comment_list[-1][0]
marshal_functions.comment("extra dummy", 'ZTF17aacscou', comment_type="info", comment_id=comment_id)
```

