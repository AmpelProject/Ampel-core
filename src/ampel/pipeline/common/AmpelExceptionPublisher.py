#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelExceptionPublisher.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 03.09.2018
# Last Modified Date: 04.09.2018
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import datetime, json, logging, time

from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.AmpelConfig import AmpelConfig

from slackclient import SlackClient
from bson import ObjectId

log = logging.getLogger()

class AmpelExceptionPublisher:
	def __init__(self):
		token = AmpelConfig.get_config('resources.slack.operator')
		self._slack = SlackClient(token)
		self._troubles = AmpelDB.get_collection('troubles', 'r')

		self._last_timestamp = ObjectId.from_datetime(datetime.datetime.now() - datetime.timedelta(days=1))

	def t3_fields(self, doc):
		fields = []
		fields.append({'title': 'Job', 'value': doc.get('jobName', None), 'short': True})
		fields.append({'title': 'Task', 'value': doc.get('taskName', None), 'short': True})
		if isinstance(doc.get('logs', None), ObjectId):
			fields.append({'title': 'logs', 'value': doc['logs'].binary.hex(), 'short': True})
		return fields

	def format_attachment(self, doc):
		fields = [{'title': 'Tier', 'value': doc['tier'], 'short': True}]
		more = doc.get('more', {})
		if doc['tier'] == 0:
			fields.append({'title': 'Section', 'value': more.get('section', None), 'short': True})
			fields.append({'title': 'tranId', 'value': more.get('tranId', None), 'short': True})
		elif doc['tier'] == 2:
			fields.append({'title': 'run_config', 'value': more.get('run_config', None), 'short': True})
			if isinstance(more.get('t2_doc', None), ObjectId):
				fields.append({'title': 't2_doc', 'value': more['t2_doc'].binary.hex(), 'short': True})
		elif doc['tier'] == 3:
			fields += self.t3_fields(more if 'jobName' in more else doc)
		if 'exception' in doc:
			text =  '```{}```'.format('\n'.join(doc['exception']))
		elif 'location' in doc:
			text = '{}: {}'.format(doc['location'], doc.get('ampelMsg', ''))
			if 'mongoUpdateResult' in doc:
				text += '\nmongoUpdateResult: `{}`'.format(doc['mongoUpdateResult'])
		else:
			text = 'Unknown exception type. Doc keys are: ```{}```'.format(doc.keys())

		attachment = {
			'fields': fields,
			'ts': int(doc['_id'].generation_time.timestamp()),
			'text': text,
			'mrkdwn_in': ['text'],
		}
		return attachment

	def publish(self):

		message = {
			'attachments': [],
			'channel': '#ampel-troubles',
			'username': 'AMPEL-live',
			'as_user': False
		}

		projection = ['_id', 'exception']
		t0 = self._last_timestamp.generation_time
		cursor = self._troubles.find({'_id': {'$gt': self._last_timestamp}})
		for doc in cursor:
			message['attachments'].append(self.format_attachment(doc))
			if len(message['attachments']) == 20:
				break

		try:
			self._last_timestamp = doc['_id']
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
			return

		count = cursor.count()
		if len(message['attachments']) < count:
			message['text'] = 'Here are the first {} exceptions. There were {} more in the last {}.'.format(len(message['attachments']), count-len(message['attachments']), time_range)
		else:
			message['text'] = 'There were {} exceptions in the last {}.'.format(len(message['attachments']), time_range)

		result = self._slack.api_call('chat.postMessage', **message)
		if not result['ok']:
			raise RuntimeError(result['error'])
		log.info("{} exceptions in the last {}".format(count, time_range))

def run():
	import schedule
	from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser
	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['logger'])
	parser.require_resource('slack', ['operator'])
	parser.add_argument('--interval', type=int, default=10, help='Check for new exceptions every INTERVAL minutes')

	args = parser.parse_args()

	atp = AmpelExceptionPublisher()
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
