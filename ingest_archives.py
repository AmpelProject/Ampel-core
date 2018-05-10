#!/usr/bin/env python

from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.alerts.TarballWalker import TarballWalker
from ampel.pipeline.t0.alerts.ZIAlertParser import ZIAlertParser
import fastavro

def _worker(idx, mongo_host, archive_host, infile):
	from ampel.archive import ArchiveDB, docker_env
	import itertools

	archive = ArchiveDB('postgresql://ampel:{}@{}/ztfarchive'.format(docker_env('POSTGRES_PASSWORD'), archive_host), use_batch_mode=True)
	mongo = 'mongodb://{}:{}@{}/'.format(docker_env('MONGO_INITDB_ROOT_USERNAME'), docker_env('MONGO_INITDB_ROOT_PASSWORD'), mongo_host)

	def loader():
		atat = TarballWalker(infile)
		for idx,fileobj in enumerate(atat.load_alerts()):
			reader = fastavro.reader(fileobj)
			alert = next(reader)
			# 10.05.18: temporarily deactivating archiving
			# archive.insert_alert(alert, idx%16, int(time.time()*1e6))
			yield alert
	def peek(iterable):
		try:
			first = next(iterable)
		except StopIteration:
			return None
		return first, itertools.chain([first], iterable)
	import time
	t0 = time.time()

	alerts = loader()
	count = 0
	while True:
		res = peek(alerts)
		if res is None:
			break
		else:
			alerts = res[1]
		processor = AlertProcessor(db_host=mongo)
		class Shim:
			def get_alerts(self):
				parser = ZIAlertParser()
				for avrodict in alerts:
					yield parser.parse(avrodict)
		chunk_size = processor.run(Shim(), console_logging=False)
		t1 = time.time()
		dt = t1-t0
		t0 = t1
		print('({} {}) {} alerts in {:.1f}s; {:.1f}/s'.format(idx, infile, chunk_size, dt, chunk_size/dt))
		count += chunk_size

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
	jobs = [executor.submit(_worker, idx, opts.host, opts.archive_host, fname) for idx,fname in enumerate(opts.infiles)]
	for future in futures.as_completed(jobs):
		print(future.result())
		count += future.result()
	duration = int(time.time()) - start_time
	print('Processed {} alerts in {:.1f} s ({:.1f}/s)'.format(count, duration, float(count)/duration))

if __name__ == "__main__":
	run_alertprocessor()
	
