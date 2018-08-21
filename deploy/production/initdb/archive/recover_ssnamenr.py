#!/usr/bin/env python

"""
Recover missing columns in the archive database from UW tarballs
"""

import datetime
import itertools
import io
import logging
import multiprocessing
import tarfile
import time
import queue

import fastavro
import requests

from ampel.pipeline.t0.alerts.TarAlertLoader import TarAlertLoader
from ampel.archive import ArchiveDB

def blobs_from_tarball(procnum, queue, date, partnership=True):
	i = 0
	try:
		if partnership:
			url = 'https://ztf:16chipsOnPalomar@ztf.uw.edu/alerts/partnership/ztf_partnership_{}.tar.gz'.format(date)
		else:
			url = 'https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz'.format(date)
		response = requests.get(url, stream=True)
		response.raise_for_status()
		
		loader = TarAlertLoader(file_obj=response.raw)
		for i, fileobj in enumerate(iter(loader)):
			queue.put(fileobj.read())
	except (tarfile.ReadError, requests.exceptions.HTTPError):
		pass
	finally:
		log.info('ztf_{}_{} finished ({} alerts)'.format(['public', 'partnership'][partnership], date, i))
		queue.put(procnum)

from sqlalchemy import select, and_, bindparam, exists
class Updater:
	def __init__(self, connection, table, ids, fields):
		self._connection = connection
		condition = and_(*(table.c[name] == bindparam('b_'+name) for name in ids))
		values = {name: bindparam('b_'+name) for name in fields}
		self._query = table.update().where(condition).values(**values)
		self._fields = set(list(ids) + list(fields))
		self._values = []

	def __len__(self):
		return len(self._values)
	
	def add(self, list_of_dicts):
		self._values += [{'b_'+k: item[k] for k in self._fields} for item in list_of_dicts]
	
	def commit(self):
		if len(self._values) == 0:
			return
		self._connection.execute(self._query, self._values)
		self._values.clear()

def split_upper_limits(prv):
	parts = [[], []]
	for element in prv:
		parts[element['candid'] is None].append(element)
	return parts

def ingest_blobs(procnum, queue, archive_url):

	db = ArchiveDB(archive_url)

	update_candidate = Updater(db._connection, db._meta.tables['candidate'], ('candid', 'pid', 'programid'), ('isdiffpos', 'ssnamenr', 'magzpscirms'))
	update_prv_candidate = Updater(db._connection, db._meta.tables['prv_candidate'], ('candid', 'pid', 'programid'), ('isdiffpos', 'ssnamenr'))
	update_upper_limit = Updater(db._connection, db._meta.tables['upper_limit'], ('jd', 'fid', 'pid', 'diffmaglim'), ('rbversion',))

	def commit():
		with db._connection.begin() as transaction:
			try:
				update_candidate.commit()
				update_prv_candidate.commit()
				update_upper_limit.commit()
				transaction.commit()
			except:
				transaction.rollback()
				raise

	while True:
		try:
			blob = queue.get()
			alert = next(fastavro.reader(io.BytesIO(blob)))
			update_candidate.add([alert['candidate']])
			dets, uls = split_upper_limits(alert['prv_candidates'])
			update_prv_candidate.add(dets)
			update_upper_limit.add(uls)
			if len(update_candidate) > 1000:
				commit()
		except:
			commit()
			raise

def ingest_blobs(procnum, queue, archive_url):
	while True:
		blob = queue.get()
		if blob is None:
			break

if __name__ == "__main__":
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument("--archive", type=str, default="localhost:5432")
	parser.add_argument("--workers", type=int, default=4, help="Number of db clients to start")
	parser.add_argument("--partnership", default=False, action="store_true")

	args = parser.parse_args()

	logging.basicConfig(level='INFO', format='%(asctime)s %(name)s:%(levelname)s: %(message)s')
	# log = logging.getLogger('ztf_{}_{}'.format(['public', 'partnership'][args.partnership], args.date))
	log = logging.getLogger()

	# Spawn 1 reader each for the public and private alerts of each night
	begin = datetime.datetime(2018,6,1)
	dates = [(begin + datetime.timedelta(i)).strftime('%Y%m%d') for i in range((datetime.datetime.now()- begin).days)]*2
	input_queue = multiprocessing.Queue(10*len(dates))
	sources = {i: multiprocessing.Process(target=blobs_from_tarball, args=(i,input_queue,date,i%2==0)) for i,date in enumerate(dates)}
	for p in sources.values():
		p.start()

	output_queues = [multiprocessing.Queue(10) for i in range(args.workers)]
	sinks = {i: multiprocessing.Process(target=ingest_blobs, args=(i,output_queues[i],args.archive)) for i in range(args.workers)}
	for p in sinks.values():
		p.start()

	try:
		t0 = time.time()
		count = 0
		chunk = 10000
		while len(sources) > 0 or not input_queue.empty():
			message = input_queue.get()
			if isinstance(message, int):
				sources[message].join()
				del sources[message]
			else:
				min(output_queues, key=lambda q: q.qsize()).put(message)
				count += 1
				if count % chunk == 0:
					dt = time.time() - t0
					log.info('{} ({:.1f} alerts/s)'.format(count, chunk/dt))
					t0 = time.time()
	finally:
		for p in sources.values():
			p.terminate()
			p.join()
		for i, q in enumerate(output_queues):
			log.info("Stopping sink {}".format(i))
			q.put(None)
			sinks[i].join()
