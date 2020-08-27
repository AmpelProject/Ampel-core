#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/metrics/AmpelStatsPublisher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 27.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import sys, json, psutil
from time import time, strftime
from typing import Optional, Dict, Any, Iterable, Container
from pymongo.collection import Collection
from pymongo.database import Database

from ampel.type import ChannelId
from ampel.log.AmpelLogger import AmpelLogger
from ampel.util.mappings import flatten_dict, unflatten_dict, get_by_path
from ampel.core.Schedulable import Schedulable
from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.AmpelDB import AmpelDB
from ampel.t2.T2RunState import T2RunState

from ampel.core.AdminUnit import AdminUnit
from ampel.model.Secret import Secret


class AmpelStatsPublisher(AdminUnit, Schedulable):
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

	channel_names: Optional[Iterable[ChannelId]] = None
	publish_to: Container[str] = ('graphite', 'mongo', 'print')
	publish_what: Container[str] = ('col_stats', 'docs_count', 'daemon', 'channels', 'archive', 'system')

	archive_auth: Secret[dict] = {'key': 'ztf/archive/reader'} # type: ignore[assignment]

	def __init__(self, **kwargs) -> None:
		"""
		:param channel_names: list of channel names, if None, stats for all avail channels will be reported.
		:param publish_to: send stats to\n
		* mongo: send metrics to dedicated mongo collection (mongodb_uri must be set)
		* graphite: send db metrics to graphite (graphite server must be defined in Ampel_config)
		* print: print db metrics to stdout
		* log: log db metrics using logger instance
		:param publish_what:\n
		* col_stats -> collection stats (size, compressedSize, indexSize)
		* docs_count -> number of documents in collections
		* daemon -> mongod stats (ram usage, number of sockets open)
		* channels -> number of transients in each channel
		* archive ->
		"""

		AdminUnit.__init__(self, **kwargs)
		# Pass custom args to Parent class constructor
		Schedulable.__init__(self,
			start_callback=self.send_all_metrics,
			stop_callback=self.send_all_metrics
		)

		# Setup logger
		self.logger = AmpelLogger.get_logger()
		self.logger.info("Setting up AmpelStatsPublisher")

		if self.channel_names is None:
			self.channels_names = tuple(
				self.context.config.get(
					'channel',
					Dict[str,Any],
					raise_exc=True
				).keys()
			)

		# update interval dict. Values in minutes
		self.update_intervals: Dict[str, Optional[int]] = {
			'col_stats': 30,
			'docs_count': 30,
			'daemon': 10,
			'channels': 10,
			'archive': 10,
			'system': 1,
		}

		# update interval dict. Values in minutes
		for key in self.update_intervals.keys():
			if key not in self.publish_what:
				self.update_intervals[key] = None

		# DB collection handles
		self.col_tran = self.context.db.get_collection("stock", "r")
		self.col_t0 = self.context.db.get_collection("t0", "r")
		self.col_t1 = self.context.db.get_collection("t1", "r")
		self.col_t2 = self.context.db.get_collection("t2", "r")
		self.col_events = self.context.db.get_collection("events", "r")
		self.col_logs = self.context.db.get_collection("logs", "r")
		self.col_troubles = self.context.db.get_collection('troubles', "r")

		# Instantiate GraphiteFeeder if required
		if 'graphite' in self.publish_to:
			from ampel.metrics.GraphiteFeeder import GraphiteFeeder
			self.graphite_feeder = GraphiteFeeder(
				self.context.config.get(
					'resource.graphite',
					str,
					raise_exc=True
				)
			)

		# Instantiate ArchiveDB if required
		if 'archive' in self.publish_what:
			from ampel.ztf.archive.ArchiveDB import ArchiveDB # type: ignore[import]
			self.archive_client = ArchiveDB(
				self.context.config.get(
					'resource.archive',
					str,
					raise_exc=True
				),
				**self.archive_auth.get()
			)

		# Schedule jobs
		self.schedule_send_metrics()

		# Dict used to save metrics previously retrieved
		self.past_items: Dict[str,Any] = {}

		# Feeback
		self.logger.info("AmpelStatsPublisher setup completed")


	def set_all_update_intervals(self, value: int) -> None:
		"""
		Convenience method.\n
		Set all update intervals ('col_stats', 'docs_count',
		'daemon', 'channels', 'archive') to the provided value.

		:param value: number of minutes between checks
		"""
		for k in self.update_intervals.keys():
			if self.update_intervals[k] is not None:
				self.update_intervals[k] = value

		self.schedule_send_metrics()


	def set_custom_update_intervals(self, d: Dict[str, Any]) -> None:
		"""
		:param d:
		- posisible keys: 'col_stats', 'docs_count', 'daemon', 'channels', 'archive'
		- possible values: number of minutes between checks (int).
		"""
		for key in d.keys():
			if key not in self.update_intervals:
				raise ValueError("Unknown key %s" % key)
			self.update_intervals[key] = d[key]

		self.schedule_send_metrics()


	def schedule_send_metrics(self) -> None:
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
		"""
		inv_map: Dict[int, Dict[str, bool]] = {}
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


	def send_all_metrics(self) -> None:
		""" Convenience method """
		self.send_metrics(True, True, True, True, False)


	def send_metrics(self,
		daemon: bool = False, col_stats: bool = False, docs_count: bool = False,
		channels: bool = False, archive: bool = False, system: bool = True,
	) -> None:
		"""
		Send/publish metrics\n
		:raises ValueError: when bad configuration was provided
		"""

		stats_dict: Dict[str, Any] = {'dbInfo': {}, 'count': {}}

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
					{'_id': {"$lt": 0}}
				).count(),

				'pps': self.col_t0.find(
					{'_id': {"$gt": 0}},
				).count(),

				't1': self.col_t1.find({}).count(),

				't2': self.col_t2.find({}).count(),

				't2States': {
					T2RunState(doc['_id']).name: doc['count'] \
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
		if channels and self.channel_names:

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
				"disk_io_counters": {
					k: v._asdict() for k, v in psutil.disk_io_counters(perdisk=True).items()
					if not (k.startswith('loop') or k.startswith('dm-'))
				},
				"net_io_counters": {
					k: v._asdict() for k, v in psutil.net_io_counters(pernic=True).items()
				}
			}

			for field in 'cpu_stats', 'swap_memory', 'virtual_memory':
				stats_dict["system"][field] = getattr(psutil, field)()._asdict()

		# Build dict with changed items only
		out_dict = {
			k: v for k, v in flatten_dict(stats_dict).items()
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
					flatten_dict(stats_dict), indent=4
				)
			)

			# pylint: disable=undefined-variable
			print("Updated metrics: %s" % json.dumps(out_dict, indent=4))
			sys.stdout.flush()


		# Log metrics using logger (logging module)
		if "log" in self.publish_to:

			self.logger.info(f"Computed metrics: {stats_dict}")
			self.logger.info(f"Updated metrics: {out_dict}")


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
								'metrics': unflatten_dict(out_dict)
							}
						}
					},
					upsert=True
				)

	@staticmethod
	def get_server_stats(
		db: Database, ret_dict: Optional[Dict] = None, suffix: str = ""
	) -> Dict:
		"""
		"""

		if ret_dict is None:
			ret_dict = {}

		server_status = db.command("serverStatus")
		for k, v in AmpelStatsPublisher.db_metrics.items():
			ret_dict[suffix + v] = get_by_path(server_status, k)

		return ret_dict


	@staticmethod
	def get_col_stats(col: Collection, suffix: str = "") -> Dict:
		""" """

		colstats = col.database.command("collstats", col.name)
		ret_dict = {}

		for key in AmpelStatsPublisher.col_stats_keys:
			ret_dict[suffix + key] = colstats[key]

		return ret_dict


	def get_tran_count(self, channel_name: Optional[ChannelId] = None) -> int:
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

def run() -> None:
	from argparse import ArgumentParser

	from ampel.core import AmpelContext
	from ampel.dev.DictSecretProvider import DictSecretProvider
	from ampel.model.UnitModel import UnitModel

	parser = ArgumentParser(add_help=True)
	parser.add_argument('config_file_path')
	parser.add_argument('--secrets', type=DictSecretProvider.load, default=None)
	parser.add_argument(
		'--publish-to', nargs='+', default=['log', 'graphite'],
		choices=['mongo', 'graphite', 'log', 'print'],
		help='Publish stats by these methods'
	)
	parser.add_argument(
		'--publish-what', nargs='+', default=['col_stats', 'docs_count', 'daemon', 'channels', 'archive', 'system'],
		choices=['col_stats', 'docs_count', 'daemon', 'channels', 'archive', 'system'],
		help='Publish these stats'
	)
	parser.add_argument('--dry-run', action='store_true', default=False,
	    help='Print exceptions rather than publishing to Slack')

	args = parser.parse_args()

	ctx = AmpelContext.load(args.config_file_path, secrets=args.secrets)

	asp = ctx.loader.new_admin_unit(
		UnitModel(unit=AmpelStatsPublisher),
		ctx,
		**{k: getattr(args, k) for k in ['publish_to', 'publish_what']}
	)

	asp.send_metrics(**{k: True for k in args.publish_what})
	asp.run()
