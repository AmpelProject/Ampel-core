#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/common/AmpelExceptionPublisher.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 03.09.2018
# Last Modified Date: 27.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import datetime
import json
import logging
import time
from typing import Any, Dict, List

from bson import ObjectId
from slack import WebClient
from slack.web.slack_response import SlackResponse

from ampel.core.AdminUnit import AdminUnit
from ampel.model.Secret import Secret

log = logging.getLogger()

class AmpelExceptionPublisher(AdminUnit):

	slack_token: Secret[str] = {'key': 'slack/operator'} # type: ignore[assignment]
	user: str = 'AMPEL-live'
	channel: str = 'ampel-troubles'
	dry_run: bool = False

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.slack = WebClient(self.slack_token.get())
		self.last_timestamp = ObjectId.from_datetime(datetime.datetime.now() - datetime.timedelta(hours=1))
		self.troubles = self.context.db.get_collection("troubles", "r")

	def t3_fields(self, doc: Dict[str, Any]) -> List[Dict[str,Any]]:
		fields = []
		if 'job' in doc:
			fields.append({'title': 'Job', 'value': doc.get('job', None), 'short': True})
		if 'task' in doc:
			fields.append({'title': 'Task', 'value': doc.get('task', None), 'short': True})
		fields.append({'title': 'Run', 'value': doc.get('run', None), 'short': True})
		return fields

	def format_attachment(self, doc: Dict[str, Any]) -> Dict[str,Any]:
		fields = [{'title': 'Tier', 'value': doc['tier'], 'short': True}]
		more = doc.get('more', {})
		if doc['tier'] == 0:
			for field in 'section', 'stock', 'run':
				fields.append({'title': field, 'value': doc.get(field, None), 'short': True})
			if 'id' in doc.get('alert', {}):
				fields.append({'title': 'alertId', 'value': doc.get('alert', {}).get('id', None), 'short': True})
		elif doc['tier'] == 2:
			fields.append({'title': 'unit', 'value': doc.get('unit', None), 'short': True})
			fields.append({'title': 'run', 'value': doc.get('run', None), 'short': True})
			t2Doc = doc.get('t2Doc', None)
			if hasattr(t2Doc, 'binary'):
				fields.append({'title': 't2Doc', 'value': t2Doc.binary.hex(), 'short': True})
		elif doc['tier'] == 3:
			fields += self.t3_fields(more if 'jobName' in more else doc)
		if 'exception' in doc:
			text =  '```{}```'.format('\n'.join(doc['exception']))
		elif 'location' in doc:
			text = '{}: {}'.format(doc['location'], doc.get('ampelMsg', ''))
			if 'mongoUpdateResult' in doc:
				text += '\nmongoUpdateResult: `{}`'.format(doc['mongoUpdateResult'])
			elif 'errDict' in doc:
				text += '```\n{}```'.format(repr(doc['errDict']))
		else:
			text = 'Unknown exception type. Doc keys are: ```{}```'.format(doc.keys())

		attachment = {
			'fields': fields,
			'ts': int(doc['_id'].generation_time.timestamp()),
			'text': text,
			'mrkdwn_in': ['text'],
		}
		return attachment

	def publish(self) -> None:

		attachments: List[Dict[str,Any]] = []
		message = {
			'attachments': attachments,
			'channel': '#'+self.channel,
			'username': self.user,
			'as_user': False
		}

		projection = ['_id', 'exception']
		t0 = self.last_timestamp.generation_time
		cursor = self.troubles.find({'_id': {'$gt': self.last_timestamp}})
		for doc in cursor:
			if len(attachments) < 20:
				attachments.append(self.format_attachment(doc))

		try:
			self.last_timestamp = doc['_id']
			dt = ObjectId.from_datetime(datetime.datetime.now()).generation_time - t0
			if dt.days > 3:
				time_range = '{} days'.format(dt.days)
			elif dt.days > 0 or dt.seconds > 2*3600:
				time_range = '{} hours'.format(int(dt.days * 24 + dt.seconds / 3600))
			elif dt.seconds > 2*60:
				time_range = '{} minutes'.format(int(dt.seconds/60))
			else:
				time_range = '{} seconds'.format(int(dt.seconds))
		except UnboundLocalError:
			if self.dry_run:
				log.info("No exceptions")
			return

		count = cursor.count()
		if len(attachments) < count:
			message['text'] = f'Here are the first {len(attachments)} exceptions. There were {count-len(attachments)} more in the last {time_range}.'
		else:
			message['text'] = f'There were {len(attachments)} exceptions in the last {time_range}.'

		if self.dry_run:
			log.info(json.dumps(message, indent=1))
		else:
			result = self.slack.api_call('chat.postMessage', data=message)
			if isinstance(result, SlackResponse):
				if not result['ok']:
					raise RuntimeError(result['error'])
			else:
				raise TypeError(f"Sync client returned a future {result}")
		log.info(f"{count} exceptions in the last {time_range}".format(count))

def run() -> None:
	from argparse import ArgumentParser

	import schedule

	from ampel.core import AmpelContext
	from ampel.dev.DictSecretProvider import DictSecretProvider
	from ampel.model.UnitModel import UnitModel

	parser = ArgumentParser(add_help=True)
	parser.add_argument('config_file_path')
	parser.add_argument('--secrets', type=DictSecretProvider.load, default=None)
	parser.add_argument('--interval', type=int, default=10, help='Check for new exceptions every INTERVAL minutes')
	parser.add_argument('--channel', type=str, default='ampel-troubles', help='Publish to this Slack channel')
	parser.add_argument('--user', type=str, default='AMPEL-live', help='Publish to as this username')
	parser.add_argument('--dry-run', action='store_true', default=False,
	    help='Print exceptions rather than publishing to Slack')

	args = parser.parse_args()

	ctx = AmpelContext.load(args.config_file_path, secrets=args.secrets)

	atp = ctx.loader.new_admin_unit(
		UnitModel(unit=AmpelExceptionPublisher),
		ctx,
		**{k: getattr(args, k) for k in ['channel', 'user', 'dry_run']}
	)

	scheduler = schedule.Scheduler()
	scheduler.every(args.interval).minutes.do(atp.publish)

	logging.basicConfig()
	atp.publish()
	while True:
		try:
			scheduler.run_pending()
			time.sleep(10)
		except KeyboardInterrupt:
			break
