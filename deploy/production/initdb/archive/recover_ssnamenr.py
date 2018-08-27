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
		if i > 0:
			log.info('ztf_{}_{} finished ({} alerts)'.format(['public', 'partnership'][partnership], date, i))
		queue.put(procnum)

from sqlalchemy import select, and_, bindparam, exists
from sqlalchemy.sql.schema import UniqueConstraint
class Updater:
	def __init__(self, connection, table, fields):
		self._connection = connection
		ids = next(c for c in table.constraints if isinstance(c, UniqueConstraint)).columns
		condition = and_(*(c == bindparam('b_'+c.name) for c in ids))
		values = {name: bindparam('b_'+name) for name in fields}
		pkey = table.primary_key.columns.values()[0]
		# NB: because we select by a set of columns with a UNIQUE constraint,
		# all rows that are locked can be safely skipped, as they are already
		# being updated in another process
		self._query = table.update().where(pkey==select([pkey]).where(condition).with_for_update(skip_locked=True)).values(**values)
		self._fields = set([c.name for c in ids] + list(fields))
		self._values = []

	def __len__(self):
		return len(self._values)
	
	def add(self, list_of_dicts):
		self._values += [{'b_'+k: item.get(k) for k in self._fields} for item in list_of_dicts]
	
	def commit(self):
		if len(self._values) == 0:
			return
		self._connection.execute(self._query, self._values)
		self._values.clear()

def split_upper_limits(prv):
	parts = [[], []]
	if prv is not None:
		for element in prv:
			parts[element['candid'] is None].append(element)
	return parts

def ingest_blobs(procnum, queue, archive_url):

	db = ArchiveDB(archive_url)
	# end implicit transaction
	db._connection.execute('COMMIT')

	update_candidate = Updater(db._connection, db._meta.tables['candidate'], ('isdiffpos', 'ssnamenr', 'magzpscirms'))
	update_prv_candidate = Updater(db._connection, db._meta.tables['prv_candidate'], ('isdiffpos', 'ssnamenr'))
	update_upper_limit = Updater(db._connection, db._meta.tables['upper_limit'], ('rbversion',))

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

'''
def ingest_blobs(procnum, queue, archive_url):
	while True:
		blob = queue.get()
		if blob is None:
			break
'''

def recover(args):
	logging.basicConfig(level='INFO', format='%(asctime)s %(name)s:%(levelname)s: %(message)s')
	log = logging.getLogger()

	# Spawn 1 reader each for the public and private alerts of each night
	begin = datetime.datetime(2018,6,1)
	dates = [(begin + datetime.timedelta(i)).strftime('%Y%m%d') for i in range((datetime.datetime.now()- begin).days)]*2
	input_queue = multiprocessing.Queue(10*args.workers)
	sources = {i: multiprocessing.Process(target=blobs_from_tarball, args=(i,input_queue,date,i%2==0)) for i,date in enumerate(dates)}
	for i, p in enumerate(sources.values()):
		if i == args.workers:
			break
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
				ready = [p for p in sources.values() if p.pid is None]
				if len(ready) > 0:
					ready[0].start()
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

def check(args):
	from astropy.time import Time
	from sqlalchemy import and_, func, select
	from sqlalchemy.sql.functions import count
	import sys
	begin = datetime.datetime(2018,6,1)
	end = begin + datetime.timedelta((datetime.datetime.now() - begin).days)
	dates = [(begin + datetime.timedelta(i)) for i in range((datetime.datetime.now()- begin).days)]
	programs = ['public', 'partnership']
	
	db = ArchiveDB(args.archive)
	Alert = db._meta.tables['alert']
	jd = func.width_bucket(Alert.c.jd, Time(begin).jd, Time(end).jd, int(Time(end).jd-Time(begin).jd))
	programid = func.width_bucket(Alert.c.programid, 0.5,2.5,2)

	q = select([jd, programid, count()]).group_by(jd, programid).order_by(jd, programid)
	total = 0
	for row in db._connection.execute(q):
		i, j, count = row
		if j < 1 or j > 2 or i < 1 or i > len(dates):
			continue
		total += count
		label = 'ztf_{}_{}'.format(programs[j-1], dates[i-1].strftime('%Y%m%d'))
		print('{} finished ({} alerts)'.format(label, count-1))
	sys.stderr.write('{} total\n'.format(total))
	return

	programs = {1: 'public', 2: 'partnership'}
	for date, bounds in zip(dates, bounds):
		for programid, name in programs.items():
			label = 'ztf_{}_{}'.format(name, date.strftime('%Y%m%d'))
			q = Alert.count(and_(Alert.c.jd > bounds[0].jd, Alert.c.jd < bounds[1].jd, Alert.c.programid == programid))
			count = db._connection.execute(q).fetchone()[0]
			if count > 0:
				print('{} finished ({} alerts)'.format(label, count-1))
				sys.stdout.flush()

if __name__ == "__main__":
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument("--archive", type=str, default="localhost:5432")

	subparsers = parser.add_subparsers()
	p = subparsers.add_parser('recover')
	p.set_defaults(func=recover)
	p.add_argument("--workers", type=int, default=4, help="Number of db clients to start")

	p = subparsers.add_parser('check')
	p.set_defaults(func=check)

	args = parser.parse_args()
	args.func(args)


