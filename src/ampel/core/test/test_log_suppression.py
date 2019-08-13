
import logging

from ampel.logging.AmpelLogger import AmpelLogger

def test_log_suppression(mocker):
	class Handler(logging.Handler):
		def emit(self, record):
			assert record.levelno > logging.DEBUG
	logger = AmpelLogger.get_unique_logger()
	logger.setLevel('INFO')
	
	handler = Handler()
	logger.addHandler(handler)

	assert logger.level > logging.DEBUG
	
	logger.debug('foo')