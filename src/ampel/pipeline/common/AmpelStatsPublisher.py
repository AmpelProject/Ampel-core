#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelStatsPublisher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 28.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from datetime import datetime
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.core.flags.AlDocType import AlDocType


class AmpelStatsPublisher(Schedulable):
	""" 

	EXAMPLE of graphite stats
	#########################

	mongod deamon stats
		"dbInfo.daemon.memRes": RAM usage
		"dbInfo.daemon.connections": number of opened TCP connection

	mongodb collection 'photo'
		"dbInfo.photo.size": size 
		"dbInfo.photo.storageSize": size on disk
		"dbInfo.photo.totalIndexSize": index size

	mongodb collection 'main'
		"dbInfo.main.size"
		"dbInfo.main.storageSize"
		"dbInfo.main.totalIndexSize"

	mongodb collection 'logs'
		"dbInfo.logs.size"
		"dbInfo.logs.storageSize"
		"dbInfo.logs.totalIndexSize"

	number of documents in collections
		"count.docs.troubles": 1
		"count.docs.photo.pps": 84
		"count.docs.photo.uls": 1583
		"count.docs.main.comps": 22
		"count.docs.main.t2s": 66
		"count.docs.main.trans": 22

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
		publish_what=['col_stats', 'docs_count', 'daemon', 'channels', 'archive'],
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
			tuple(AmpelConfig.get_config('channels').keys()) if channel_names is None 
			else channel_names
		)

		# update interval dict. Values in minutes
		self.update_intervals = {
			'col_stats': 30, 
			'docs_count': 30, 
			'daemon': 10, 
			'channels': 10,
			'archive': 10
		}

		# update interval dict. Values in minutes
		for key in self.update_intervals.keys(): 
			if key not in publish_what:
				self.update_intervals[key] = None

		# Which stats to publish (see doctring)
		self.publish_to = publish_to

		# DB collection handles
		self.col_photo = AmpelDB.get_collection("photo", "r")
		self.col_main = AmpelDB.get_collection("main", "r")
		self.col_runs = AmpelDB.get_collection("runs", "r")
		self.col_logs = AmpelDB.get_collection("jobs", "r")
		self.col_troubles = AmpelDB.get_collection('troubles', "r")

		# Projections
		self.id_proj = {'_id': 1}
		self.tran_proj = {'tranId': 1, '_id': 0}

		# Optional import
		if 'print' in publish_to:
			import json

		# Instanciate GraphiteFeeder if required
		if 'graphite' in publish_to:
			from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
			self.graphite_feeder = GraphiteFeeder(
				AmpelConfig.get_config('resources.graphite.default')
			)

		# Instanciate ArchiveDB if required
		if 'archive' in publish_what:
			from ampel.archive.ArchiveDB import ArchiveDB
			self.archive_client = ArchiveDB(
				AmpelConfig.get_config('resources.archive.reader')
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
		self, daemon=False, col_stats=False, docs_count=False, channels=False, archive=False
	):
		"""
		Send/publish metrics\n
		:raises ValueError: when bad configuration was provided
		:returns: None
		"""

		main_col = AmpelDB.get_collection("main", "r")
		photo_col = AmpelDB.get_collection("photo", "r")
		stats_dict = {'dbInfo': {}, 'count': {}}

		if not any([daemon, col_stats, docs_count, channels]):
			raise ValueError("Bad arguments")


		# GATHER SECTION
		################

		# Stats from mongod running daemon (such as RAM usage)
		if daemon:
			stats_dict['dbInfo']['daemon'] = self.get_server_stats(
				main_col.database
			)

		# Stats related to mongo collections (colstats)
		if col_stats:
			stats_dict['dbInfo']['photo'] = self.get_col_stats(self.col_photo)
			stats_dict['dbInfo']['main'] = self.get_col_stats(self.col_main)
			stats_dict['dbInfo']['logs'] = self.get_col_stats(self.col_logs)


		# Counts the number of <documents> in various collections
		if docs_count:

			stats_dict['count']['docs'] = {

				'troubles': self.col_troubles.find({}).count(),

				'photo': {

					'pps': photo_col.find(
						{'_id': {"$gt" : 0}}, 
						self.id_proj
					).count(),

					'uls': photo_col.find(
						{'_id': {"$lt" : 0}}, 
						self.id_proj
					).count()
				},

				'main': {

					'comps': self.col_main.find(
						{
							'tranId': {"$gt" : 1}, 
							'alDocType': AlDocType.COMPOUND
						},
						self.tran_proj
					).count(),

					't2s': self.col_main.find(
						{
							'tranId': {"$gt" : 1}, 
							'alDocType': AlDocType.T2RECORD
						},
						self.tran_proj
					).count(),

					'trans': self.col_main.find(
						{	
							'tranId': {"$gt" : 1}, 
							'alDocType': AlDocType.TRANSIENT
						},
						self.tran_proj
					).count()
				}
			}


#		if alerts_count:
#
#			res = next(
#				self.col_runs.aggregate(
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


		# Build dict with changed items only
		out_dict = {
			k:v for k,v in AmpelUtils.flatten_dict(stats_dict).items() 
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
				self.col_runs.update_one(
					{'_id': int(datetime.today().strftime('%Y%m%d'))},
					{
						'$push': {
							'jobs': {
								'name': 'asp',
								'dt': datetime.utcnow().timestamp(),
								'metrics': AmpelUtils.unflatten_dict(out_dict)
							}
						}
					},
					upsert=True
				)


	def get_server_stats(self, db, ret_dict=None, suffix=""):
		"""
		"""

		if ret_dict == None:
			ret_dict = {}

		server_status = db.command("serverStatus")
		for k, v in AmpelStatsPublisher.db_metrics.items():
			ret_dict[suffix + v] = AmpelUtils.get_by_path(server_status, k)

		return ret_dict


	def get_col_stats(self, col, suffix=""):
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

			return self.col_main.find(
				{
					'tranId': {'$gt': 1},
					'alDocType': AlDocType.TRANSIENT
				},
				{'tranId': 1, '_id': 0}
			).count()

		else:

			return self.col_main.find(
				{
					'tranId': {'$gt': 1},
					'alDocType': AlDocType.TRANSIENT, 
					'channels': channel_name
				},
				{'tranId': 1, '_id': 0}
			).count()

def run():

	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['logger'])
	parser.require_resource('archive', ['reader'])
	parser.require_resource('graphite')
	parser.parse_args()

	asp = AmpelStatsPublisher(publish_to=['log', 'graphite'])
	asp.send_all_metrics()
	asp.run()
