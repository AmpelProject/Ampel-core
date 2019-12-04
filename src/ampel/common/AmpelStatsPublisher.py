#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/common/AmpelStatsPublisher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 20.08.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, json, psutil
from time import time, strftime
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.common.AmpelUtils import AmpelUtils
from ampel.common.Schedulable import Schedulable
from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.AmpelDB import AmpelDB
from ampel.flags.T2RunStates import T2RunStates

class AmpelStatsPublisher(Schedulable):
	""" 

	EXAMPLE of graphite stats
	#########################

	mongod deamon stats
		"dbInfo.daemon.memRes": RAM usage
		"dbInfo.daemon.connections": number of opened TCP connection

	mongodb collection 't0'
		"dbInfo.t0.size": size 
		"dbInfo.t0.storageSize": size on disk
		"dbInfo.t0.totalIndexSize": index size

	mongodb collection 't2'
		"dbInfo.t2.size"
		"dbInfo.t2.storageSize"
		"dbInfo.t2.totalIndexSize"

	mongodb collection 'logs'
		"dbInfo.logs.size"
		"dbInfo.logs.storageSize"
		"dbInfo.logs.totalIndexSize"

	number of documents in collections
		"count.docs.troubles": 1
		"count.docs.t0": 1667
		"count.docs.pps": 84
		"count.docs.uls": 1583
		"count.docs.t1": 22
		"count.docs.t2": 66

	Channel specific stats
		"count.chans.HU_SN1": 0
		"count.chans.HU_SN2": 0
	"""

	# mongod serverStatus key values to publish
	db_metrics = {
		"mem.resident": "memRes",
		"connections.current": "connections"
	}

	# mongod colStats key values to publish
	col_stats_keys = ('size', 'storageSize', 'totalIndexSize')


	def __init__(
		self, channel_names=None, 
		publish_to=['graphite', 'mongo', 'print'],
		publish_what=['col_stats', 'docs_count', 'daemon', 'channels', 'archive', 'system'],
	):
		"""
		:param list(str) channel_names: list of channel names, if None, stats for all avail channels will be reported. 
		:param list(str) publish_to: send stats to\n
		  * mongo: send metrics to dedicated mongo collection (mongodb_uri must be set)
		  * graphite: send db metrics to graphite (graphite server must be defined in Ampel_config)
		  * print: print db metrics to stdout
		  * log: log db metrics using logger instance
		:param list(str) publish_what:\n
		  * col_stats -> collection stats (size, compressedSize, indexSize)
		  * docs_count -> number of documents in collections
		  * daemon -> mongod stats (ram usage, number of sockets open)
		  * channels -> number of transients in each channel
		  * archive -> 
		"""

		# Pass custom args to Parent class constructor
		Schedulable.__init__(self, 
			start_callback=self.send_all_metrics, 
			stop_callback=self.send_all_metrics
		)

		# Setup logger
		self.logger = AmpelLogger.get_unique_logger()
		self.logger.info("Setting up AmpelStatsPublisher")

		# Load provided channels or all channels defined in AmpelConfig
		self.channel_names = (
			tuple(AmpelConfig.get('channel').keys()) if channel_names is None 
			else channel_names
		)

		# update interval dict. Values in minutes
		self.update_intervals = {
			'col_stats': 30, 
			'docs_count': 30, 
			'daemon': 10, 
			'channels': 10,
			'archive': 10,
			'system': 1,
		}

		# update interval dict. Values in minutes
		for key in self.update_intervals.keys(): 
			if key not in publish_what:
				self.update_intervals[key] = None

		# Which stats to publish (see doctring)
		self.publish_to = publish_to

		# DB collection handles
		self.col_tran = AmpelDB.get_collection("stock", "r")
		self.col_t0 = AmpelDB.get_collection("t0", "r")
		self.col_t1 = AmpelDB.get_collection("t1", "r")
		self.col_t2 = AmpelDB.get_collection("t2", "r")
		self.col_events = AmpelDB.get_collection("events", "r")
		self.col_logs = AmpelDB.get_collection("logs", "r")
		self.col_troubles = AmpelDB.get_collection('troubles', "r")

		# Instanciate GraphiteFeeder if required
		if 'graphite' in publish_to:
			from ampel.common.GraphiteFeeder import GraphiteFeeder
			self.graphite_feeder = GraphiteFeeder(
				AmpelConfig.get('resource.graphite.default')
			)

		# Instanciate ArchiveDB if required
		if 'archive' in publish_what:
			from ampel.ztf.archive.ArchiveDB import ArchiveDB
			self.archive_client = ArchiveDB(
				AmpelConfig.get('resource.archive.reader')
			)

		# Schedule jobs
		self.schedule_send_metrics()

		# Dict used to save metrics previously retrieved
		self.past_items = {}

		# Feeback
		self.logger.info("AmpelStatsPublisher setup completed")


	def set_all_update_intervals(self, value):
		"""
		Convenience method.\n
		Sets all update intervals ('col_stats', 'docs_count', 
		'daemon', 'channels', 'archive') to provided value.

		:param int value: number of minutes between checks.
		:returns: None
		"""
		for k in self.update_intervals.keys():
			if self.update_intervals[k] is not None:
				self.update_intervals[k] = value

		self.schedule_send_metrics()


	def set_custom_update_intervals(self, d):
		"""
		:param dict d: dict instance containing one or more items:\n
			* posisible keys: 'col_stats', 'docs_count', 'daemon', 'channels', 'archive'
			* possible values: number of minutes between checks (int).
		:returns: None
		"""
		for key in d.keys():
			if key not in self.update_intervals:
				raise ValueError("Unknown key %s" % key)
			self.update_intervals[key] = d[key]

		self.schedule_send_metrics()


	def schedule_send_metrics(self):
		"""
		Converts 'update_intervals': {
			'channels': 5, 'col_stats': 5, 'deamon': 2, 'docs_count': 10
		}
		into: {
			  2: {'deamon': True},
 		 	  5: {'channels': True, 'col_stats': True},
 		 	  10: {'docs_count': True}
		}
		and schedule method send_metrics accordingly\n
		:returns: None
		"""
		inv_map = {}
		for k, v in self.update_intervals.items():
			if v is None:
				continue
			if v in inv_map:
				inv_map[v][k] = True
			else:
				inv_map[v] = {k: True}

		# Schedule jobs
		scheduler = self.get_scheduler()
		scheduler.clear()

		for interval in inv_map.keys():
			scheduler.every(interval).minutes.do(
				self.send_metrics, 
				**inv_map[interval]
			)


	def send_all_metrics(self):
		"""
		Convenience method\n
		:returns: None
		"""
		self.send_metrics(True, True, True, True, False)


	def send_metrics(
		self, daemon=False, col_stats=False, docs_count=False, 
		channels=False, archive=False, system=True,
	):
		"""
		Send/publish metrics\n
		:raises ValueError: when bad configuration was provided
		:returns: None
		"""

		stats_dict = {'dbInfo': {}, 'count': {}}

		if not any([daemon, col_stats, docs_count, channels, archive, system]):
			raise ValueError("Bad arguments")


		# GATHER SECTION
		################

		# Stats from mongod running daemon (such as RAM usage)
		if daemon:
			stats_dict['dbInfo']['daemon'] = self.get_server_stats(
				self.col_t2.database
			)

		# Stats related to mongo collections (colstats)
		if col_stats:
			stats_dict['dbInfo']['tran'] = self.get_col_stats(self.col_tran)
			stats_dict['dbInfo']['t0'] = self.get_col_stats(self.col_t0)
			stats_dict['dbInfo']['t1'] = self.get_col_stats(self.col_t1)
			stats_dict['dbInfo']['t2'] = self.get_col_stats(self.col_t2)
			stats_dict['dbInfo']['logs'] = self.get_col_stats(self.col_logs)


		# Counts the number of <documents> in various collections
		if docs_count:

			stats_dict['count']['docs'] = {

				'troubles': self.col_troubles.find({}).count(),

				'tran': self.col_tran.find({}).count(),

				't0': self.col_t0.find({}).count(),

				'uls': self.col_t0.find(
					{'_id': {"$lt" : 0}}
				).count(),

				'pps': self.col_t0.find(
					{'_id': {"$gt" : 0}}, 
				).count(),

				't1': self.col_t1.find({}).count(),

				't2': self.col_t2.find({}).count(),

				't2States': {
					T2RunStates(doc['_id']).name: doc['count'] \
					for doc in self.col_t2.aggregate([
						{'$match': {}},
						{'$project':
							{'runState': 1}
						},
						{'$group':
							{
								'_id': '$runState',
								'count': {'$sum': 1}
							}
						}
					])
				}
			}


#		if alerts_count:
#
#			res = next(
#				self.col_events.aggregate(
#					[
#						#{
#						#	"$match": { 
#						#		# TODO: add time constrain here 
#								# example: since last AMPEL start
#						#	}
#						#},
#						{
#   							"$group": {
#            					"_id": 1,
#            					"alProcessed": {
#									"$sum": "$jobs.job.metrics.count.alerts"
#								}
#							}
#						}
#					]
#				), None
#			)
#
#			if res is None: 
#				# TODO: something
#				pass
#
#			stats_dict['alertsCount'] = res['alProcessed']


		# Channel specific metrics
		if channels:

			# get number of transient docs for each channel
			stats_dict["count"]['chans'] = {}
			for chan_name in self.channel_names:
				stats_dict["count"]['chans'][chan_name] = self.get_tran_count(chan_name)


		# Channel specific metrics
		if archive:
			stats_dict["archive"] = {}
			stats_dict["archive"]["tables"] = self.archive_client.get_statistics()

		if system:
			stats_dict["system"] = {
				"cpu_percent": psutil.cpu_percent(),
				"disk_io_counters": {k:v._asdict() for k, v in psutil.disk_io_counters(perdisk=True).items() if not (k.startswith('loop') or k.startswith('dm-'))},
				"net_io_counters": {k:v._asdict() for k, v in psutil.net_io_counters(pernic=True).items()}
			}
			for field in 'cpu_stats', 'swap_memory', 'virtual_memory':
				stats_dict["system"][field] = getattr(psutil, field)()._asdict()

		# Build dict with changed items only
		out_dict = {
			k:v for k, v in AmpelUtils.flatten_dict(stats_dict).items() 
			if self.past_items.get(k) != v
		}

		# Update internal dict of past items
		for k, v in out_dict.items():
			self.past_items[k] = v



		# PUBLISH SECTION
		#################

		# Print metrics to stdout
		if "print" in self.publish_to:

			# pylint: disable=undefined-variable
			print(
				"Computed metrics: %s" % json.dumps(
					AmpelUtils.flatten_dict(stats_dict), indent=4
				)
			)

			# pylint: disable=undefined-variable
			print("Updated metrics: %s" % json.dumps(out_dict, indent=4))
			sys.stdout.flush()


		# Log metrics using logger (logging module)
		if "log" in self.publish_to:

			self.logger.info("Computed metrics: %s" % str(stats_dict))
			self.logger.info("Updated metrics: %s" % str(out_dict))


		# Publish metrics to graphite
		if "graphite" in self.publish_to:

			if len(out_dict) == 0:
				self.logger.info("Skipping graphite update")
			else:
				self.logger.info("Sending stats to graphite")
				self.graphite_feeder.add_stats({'statspublisher': stats_dict})
				self.graphite_feeder.send()


		# Publish metrics to mongo 'runs' collection
		if "mongo" in self.publish_to:

			if len(out_dict) == 0:
				self.logger.info("Skipping mongo update")

			else:
				# Record job info into DB
				self.col_events.update_one(
					{'_id': int(strftime('%Y%m%d'))},
					{
						'$push': {
							'events': {
								'name': 'statsPublisher',
								'dt': time(),
								'metrics': AmpelUtils.unflatten_dict(out_dict)
							}
						}
					},
					upsert=True
				)

	@staticmethod
	def get_server_stats(db, ret_dict=None, suffix=""):
		"""
		"""

		if ret_dict is None:
			ret_dict = {}

		server_status = db.command("serverStatus")
		for k, v in AmpelStatsPublisher.db_metrics.items():
			ret_dict[suffix + v] = AmpelUtils.get_by_path(server_status, k)

		return ret_dict


	@staticmethod
	def get_col_stats(col, suffix=""):
		"""
		"""
		colstats = col.database.command("collstats", col.name)
		ret_dict = {}

		for key in AmpelStatsPublisher.col_stats_keys:
			ret_dict[suffix + key] = colstats[key]

		return ret_dict

	
	def get_tran_count(self, channel_name=None):
		"""
		get number of unique transient in collection.
		Query should be covered.

		channel_name:
		-> if None: get total number of unique transient in collection
		-> if specified: get total # of unique transient for the specified channel in collection
		"""

		if channel_name is None:

			return self.col_tran \
				.find({}, {'_id': 1}) \
				.count()

		return self.col_tran \
			.find(
				{
					'_id': {'$gt': 1},
					'channels': channel_name
				},
				{'_id': 1}
			) \
			.hint(
				# Hint is very important here. It ensures the query is covered
				'_id_1_channels_1'
			) \
			.count()

def run():

	from ampel.config.AmpelArgumentParser import AmpelArgumentParser
	parser = AmpelArgumentParser()
	parser.add_argument('--publish-to', nargs='+', default=['log', 'graphite'],
	    choices=['mongo', 'graphite', 'log', 'print'],
	    help='Publish stats by these methods')
	parser.add_argument('--publish-what', nargs='+', default=['col_stats', 'docs_count', 'daemon', 'channels', 'archive', 'system'],
	    choices=['col_stats', 'docs_count', 'daemon', 'channels', 'archive', 'system'],
	    help='Publish these stats')
	args, _ = parser.parse_known_args()
	if 'archive' in args.publish_what:
		parser.require_resource('archive', ['reader'])
	if 'graphite' in args.publish_to:
		parser.require_resource('graphite')
	if 'mongo' in args.publish_to or set(args.publish_what).difference(['archive', 'system']):
		parser.require_resource('mongo', ['logger'])
	args = parser.parse_args()

	asp = AmpelStatsPublisher(publish_to=args.publish_to, publish_what=args.publish_what)
	asp.send_metrics(**{k:True for k in args.publish_what})
	asp.run()
