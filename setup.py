from setuptools import setup
setup(name='Ampel-core',
      version='0.4.0',
      package_dir={'':'src'},
      packages=[
          'ampel.core',
          'ampel.core.abstract',
          'ampel.core.flags',
          'ampel.pipeline',
          'ampel.pipeline.config',
          'ampel.pipeline.common',
          'ampel.pipeline.db',
          'ampel.pipeline.db.query',
          'ampel.pipeline.logging',
          'ampel.pipeline.t0',
          'ampel.pipeline.t0.alerts',
          'ampel.pipeline.t0.filters',
          'ampel.pipeline.t0.ingesters',
          'ampel.pipeline.t2',
          'ampel.pipeline.t3',
          'ampel.test',
          'ampel.utils',
          'ampel.view',
      ],
      package_data = {
          '': [
              '*.json'
          ],
          'ampel.test': [
              'test-data/*.json',
              'deploy/production/initdb/*/*.sql',
              'deploy/prodution/initdb/*/*.sh'
          ]
      },
      entry_points = {
          'console_scripts' : [
              'ampel-alertprocessor = ampel.pipeline.t0.AlertProcessor:run_alertprocessor',
              'ampel-followup = ampel.pipeline.t0.DelayedT0Controller:run',
              'ampel-statspublisher = ampel.pipeline.common.AmpelStatsPublisher:run',
              'ampel-t2 = ampel.pipeline.t2.T2Controller:run',
              'ampel-t3 = ampel.pipeline.t3.T3Controller:main',
              'ampel-check-broker = ampel.pipeline.t0.ZIAlertFetcher:list_kafka',
              'ampel-archive-topic = ampel.pipeline.t0.ZIAlertFetcher:archive_topic',
          ],
          'ampel.pipeline.resources' : [
              'mongo = ampel.pipeline.common.resources:LiveMongoURI',
              'graphite = ampel.pipeline.common.resources:Graphite',
              'archive = ampel.archive.resources:ArchiveDBURI'
          ]
      }
)
