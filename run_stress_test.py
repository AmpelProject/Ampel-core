#!/usr/bin/env python

from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.alerts.TarAlertLoader import TarAlertLoader
from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper

def _worker(idx, mongo_host, archive_host, infile):

	from ampel.archive import docker_env
	import itertools

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
	processor = AlertProcessor(db_host=mongo, printme=infile)

	while alert_processed == AlertProcessor.iter_max:
		t0 = time.time()
		alert_processed = processor.run(alert_supplier, console_logging=False)
		t1 = time.time()
		dt = t1-t0
		print('({} {}) {} alerts in {:.1f}s; {:.1f}/s'.format(idx, infile, alert_processed, dt, alert_processed/dt))
		count += alert_processed

	return count

def run_alertprocessor():
	import os, time, uuid
	from concurrent import futures
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument('--host', default='mongo:27017')
	parser.add_argument('--archive-host', default='archive:5432')
	parser.add_argument('--procs', type=int, default=1, help='Number of processes to start')
	parser.add_argument('infiles', nargs='+')
	
	opts = parser.parse_args()
	
	executor = futures.ProcessPoolExecutor(opts.procs)

	start_time = time.time()
	count = 0
	jobs = [executor.submit(_worker, idx, opts.host, opts.archive_host, fname) for idx,fname in enumerate(opts.infiles[:])]
	for future in futures.as_completed(jobs):
		print(future.result())
		count += future.result()
	duration = int(time.time()) - start_time
	print('Processed {} alerts in {:.1f} s ({:.1f}/s)'.format(count, duration, float(count)/duration))

if __name__ == "__main__":
	run_alertprocessor()
	
