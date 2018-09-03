
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
			fields = [{'title': 'Tier', 'value': doc['tier'], 'short': True}]
			more = doc.get('more', {})
			if doc['tier'] == 0:
				fields.append({'title': 'Section', 'value': more.get('section', None), 'short': True})
				fields.append({'title': 'tranId', 'value': more.get('tranId', None), 'short': True})
			elif doc['tier'] == 3:
				fields.append({'title': 'Job', 'value': more.get('jobName', None), 'short': True})
				fields.append({'title': 'Task', 'value': more.get('taskName', None), 'short': True})

			attachment = {
				'fields': fields,
				'ts': int(doc['_id'].generation_time.timestamp()),
				'text': '```{}```'.format('\n'.join(doc['exception'])),
				'mrkdwn_in': ['text'],
			}
			message['attachments'].append(attachment)
			if len(message['attachments']) == 20:
				break
			# break

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
			message['text'] = 'Hi, we\'re testing exception publishing. There were {} exceptions in the last {}.'.format(len(message['attachments']), time_range)

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
	while True:
		try:
			scheduler.run_pending()
			time.sleep(10)
		except KeyboardInterrupt:
			break
