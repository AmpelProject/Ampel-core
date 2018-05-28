#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelStatsPublisher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 28.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce
import schedule, time, threading, signal
from ampel.pipeline.db.DBWired import DBWired
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.flags.AlDocTypes import AlDocTypes


class AmpelStatsPublisher(DBWired, Schedulable):
	""" 
	"""

	# mongod serverStatus key values
	db_metrics = {
		"mem.resident": "memRes",
		"connections.current": "connections"
		#"metrics.document.deleted": "docDel",
		#"metrics.document.inserted": "docIns",
		#"metrics.document.returned": "docRet",
		#"metrics.document.updated": "docUpd"
	}

	col_stats_keys = (
		'count', 'size', 'storageSize', 'totalIndexSize'
	)


	def __init__(
		self, config_db=None, central_db=None, mongo_uri=None, 
		channel_names=None, publish_stats=['graphite', 'mongo', 'print'],
		update_intervals = {'col_stats': 5, 'docs_count': 10, 'daemon': 2, 'channels': 5}
	):
		"""
		Parameters:
		'config_db': see ampel.pipeline.db.DBWired.plug_config_db() docstring
		'central_db': see ampel.pipeline.db.DBWired.plug_central_db() docstring
		'publish_stats': send performance stats to:
		  * mongo: send metrics to dedicated mongo collection (mongo_uri must be set)
		  * graphite: send db metrics to graphite (graphite server must be defined in Ampel_config)
		  * print: print db metrics to stdout
		'mongo_uri': dns name or ip address (plus optinal port) of the server hosting mongod
		  example: mongodb://user:password@localhost:27017
		'channel_names': list of channel names (string). 
		  If None, stats for all avail channels will be reported. 
		'colStats_interval': 
		'docsCount_interval': 
		"""

		# Pass custom args to Parent class constructor
		Schedulable.__init__(self, 
			start_callback=self.send_all_metrics, 
			stop_callback=self.send_all_metrics
		)

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.logger.info("Setting up AmpelStatsPublisher")


		# Setup instance variable referencing ampel databases
		self.plug_databases(self.logger, mongo_uri, config_db, central_db)

		if channel_names is None:
			self.channel_names = tuple(
				doc['_id'] for doc in self.config_db['channels'].find({})
			)
		else:
			self.channel_names = channel_names

		# Which stats to publish (see doctring)
		self.publish_stats = publish_stats

		# Instantiate GraphiteFeeder class if so required
		if "graphite" in publish_stats:
			self.gfeeder = GraphiteFeeder(self.global_config['graphite'])

		# Projections
		self.id_proj = {'_id': 1}
		self.tran_proj = {'tranId': 1, '_id': 0}

		# Converts:
		# {'channels': 5, 'col_stats': 5, 'deamon': 2, 'docs_count': 10}
		# into:
		# {
		#	  2: {'deamon': True},
 		# 	  5: {'channels': True, 'col_stats': True},
 		# 	  10: {'docs_count': True}
		# }
		inv_map = {}
		for k, v in update_intervals.items():
			if v in inv_map:
				inv_map[v][k] = True
			else:
				inv_map[v] = {k: True}

		# Schedule jobs
		scheduler = self.get_scheduler()
		for interval in inv_map.keys():
			scheduler.every(interval).minutes.do(
				self.send_metrics, 
				**inv_map[interval]
			)

		self.logger.info("AmpelStatsPublisher setup completed")


	def send_all_metrics(self):
		"""
		"""
		self.send_metrics(True, True, True, True)


	def send_metrics(self, daemon=False, col_stats=False, docs_count=False, channels=False):
		"""
		"""

		main_col = self.get_main_col()
		photo_col = self.get_photo_col()
		dbinfo_dict = {}

		if not any([daemon, col_stats, docs_count, channels]):
			raise ValueError("Bad arguments")

		if daemon:

			dbinfo_dict['daemon'] = self.get_server_stats(
				main_col.database
			)


		if col_stats:

			dbinfo_dict['colStats'] = {
				'jobs': self.get_col_stats(self.get_job_col()),
				'photo': self.get_col_stats(photo_col),
				'main': self.get_col_stats(main_col)
			}


		if docs_count:

			dbinfo_dict['docsCount'] = {

				'troubles': self.get_trouble_col().find({}).count(),

				'transients': {

					'pps': photo_col.find(
						{'_id': {"$gt" : 0}}, 
						self.id_proj
					).count(),

					'uls': photo_col.find(
						{'_id': {"$lt" : 0}}, 
						self.id_proj
					).count(),

					'compounds': main_col.find(
						{
							'tranId': {"$gt" : 1}, 
							'alDocType': AlDocTypes.COMPOUND
						},
						self.tran_proj
					).count(),

					't2s': main_col.find(
						{
							'tranId': {"$gt" : 1}, 
							'alDocType': AlDocTypes.T2RECORD
						},
						self.tran_proj
					).count(),

					'trans': main_col.find(
						{	
							'tranId': {"$gt" : 1}, 
							'alDocType': AlDocTypes.TRANSIENT
						},
						self.tran_proj
					).count()
				}
			}

		if len(dbinfo_dict) > 0:
			stat_dict = {'dbinfo': dbinfo_dict} 

		# Channel specific metrics
		if channels:

			count_dict = {} 

			for chan_name in self.channel_names:
				count_dict[chan_name] = self.get_tran_count(main_col, chan_name)

			stat_dict['count'] = {'db': count_dict}

		# Publish metrics to graphite
		if "graphite" in self.publish_stats:
			self.logger.info("Sending stats to graphite")
			self.gfeeder.add_stats(stat_dict)
			self.gfeeder.send()


		# Publish metrics to mongo
		if "mongo" in self.publish_stats:
			# WILL BE IMPLEMENTED SOON
			pass

		if "print" in self.publish_stats:
			print(stat_dict)


	def get_server_stats(self, db, ret_dict=None, suffix=""):
		"""
		"""

		if ret_dict == None:
			ret_dict = {}

		server_status = self.get_main_col().database.command("serverStatus")
		for k, v in AmpelStatsPublisher.db_metrics.items():
			ret_dict[suffix + v] = reduce(dict.get, k.split("."), server_status)

		return ret_dict


	def get_col_stats(self, col, ret_dict=None, suffix=""):
		"""
		"""
		colstats = col.database.command(
			"collstats", col.name
		)

		if ret_dict == None:
			ret_dict = {}

		for key in AmpelStatsPublisher.col_stats_keys:
			ret_dict[suffix + key] = colstats[key]

		return ret_dict

	
	def get_tran_count(self, col, channel_name=None):
		"""
		get number of unique transient in collection.
		Query should be covered.

		channel_name:
		-> if None: get total number of unique transient in collection
		-> if specified: get total # of unique transient for the specified channel in collection
		"""

		if channel_name is None:

			return col.find(
				{
					'tranId': {'$gt': 1},
					'alDocType': AlDocTypes.TRANSIENT
				},
				{'tranId': 1, '_id': 0}
			).count()

		else:

			return col.find(
				{
					'tranId': {'$gt': 1},
					'alDocType': AlDocTypes.TRANSIENT, 
					'channels': channel_name
				},
				{'tranId': 1, '_id': 0}
			).count()
