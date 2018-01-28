from ampel.contrib.hu.examples.t2.T2ExamplePolyFit import T2ExamplePolyFit
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

# quick n dirty trick to have a functioning self.logger in example construction of T2ExamplePolyFit
T2ExamplePolyFit.logger = LoggingUtils.get_logger()
