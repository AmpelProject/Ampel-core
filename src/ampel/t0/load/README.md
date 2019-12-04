DirAlertLoader example:

```python
# Note: an alert_processor instance is required

# Container class allowing to conveniently iterate over local avro files 
alert_loader = DirAlertLoader(alert_processor.logger)

# input directory where alerts are stored (alert files are loaded sorted by date)
alert_loader.set_folder("/path/to/alerts/")

# extension of alert files (default: *.avro. Alternative: *.json)
alert_loader.set_extension("*.avro")
		
# limit the number of files to be loaded
alert_loader.set_max_entries(1000)

alert_processor.source_alert_supplier(alert_loader)
		
alert_processor.logger.info("Processing files in folder: %s" % base_dir)

ret = AlertProcessor.iter_max
count = 0

while ret == AlertProcessor.iter_max:
	ret = alert_processor.run()
	count += ret
```
