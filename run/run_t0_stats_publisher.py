from ampel.pipeline.common.AmpelStatsPublisher import AmpelStatsPublisher
from ampel.archive import docker_env

mongo_uri = 'mongodb://{}:{}@{}/'.format(
	docker_env('MONGO_INITDB_ROOT_USERNAME'),
	docker_env('MONGO_INITDB_ROOT_PASSWORD'),
	"localhost:2004" # TODO improve
)

asp = AmpelStatsPublisher(
	mongo_uri=mongo_uri, 
	publish_stats=['print', 'graphite']
)
asp.start()
