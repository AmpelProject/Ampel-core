#!/usr/bin/env python

from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.alerts.TarAlertLoader import TarAlertLoader
from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
from ampel.archive import docker_env


def _worker(mongo_host, infile):

	mongo = 'mongodb://{}:{}@{}/'.format(
		docker_env('MONGO_INITDB_ROOT_USERNAME'), 
		docker_env('MONGO_INITDB_ROOT_PASSWORD'), 
		mongo_host
	)

	import time
	count = 0
	alert_processed = AlertProcessor.iter_max
	tar_loader = TarAlertLoader(tar_path=infile)
	alert_supplier = AlertSupplier(tar_loader, ZIAlertShaper(), serialization="avro")
	processor = AlertProcessor(db_host=mongo)

	while alert_processed == AlertProcessor.iter_max:
		t0 = time.time()
		alert_processed = processor.run(alert_supplier, console_logging=False)
		t1 = time.time()
		dt = t1-t0
		print('({}) {} alerts in {:.1f}s; {:.1f}/s'.format(infile, alert_processed, dt, alert_processed/dt))
		count += alert_processed

	return count


def run_alertprocessor():
	import os, time, uuid
	from concurrent import futures
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument('--host', default='mongo:27017')
	parser.add_argument('infile')
	
	opts = parser.parse_args()
	
	start_time = time.time()
	count = _worker(opts.host, opts.infile) 
	duration = int(time.time()) - start_time
	print('Processed {} alerts in {:.1f} s ({:.1f}/s)'.format(count, duration, float(count)/duration))

if __name__ == "__main__":
	run_alertprocessor()
	
