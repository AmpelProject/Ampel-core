from ampel.contrib.hu.examples.t0.ExampleFilter import ExampleFilter
import logging, sys

logger = logging.getLogger("Ampel")
logger.propagate = False

shandler = logging.StreamHandler(sys.stdout)
shandler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
	'%(asctime)s %(funcName)s() %(levelname)s %(message)s',
	"%Y-%m-%d %H:%M:%S"
)
shandler.setFormatter(formatter)

logger.addHandler(shandler)
logger.setLevel(logging.DEBUG)

# quick n dirty trick to have a functioning self.logger 
# in example construction of ExampleFilter
ExampleFilter.logger = logger
