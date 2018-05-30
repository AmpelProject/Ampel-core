from distutils.core import setup
setup(name='ampel',
      version='0.1',
      package_dir={'':'src'},
      packages=['ampel'],
      entry_points = {
          'console_scripts' : [
              'ampel-alertprocessor = ampel.pipeline.t0.AlertProcessor:run_alertprocessor',
              'ampel-statspublisher = ampel.pipeline.common.AmpelStatsPublisher:run',
              'ampel-init-db = ampel.pipeline.t0.AlertProcessor:init_db',
              'ampel-init-archive = ampel.archive:init_db',
              'ampel-check-broker = ampel.pipeline.t0.ZIAlertFetcher:list_kafka',
              'ampel-archive-topic = ampel.pipeline.t0.ZIAlertFetcher:archive_topic',
          ]
      }
)
