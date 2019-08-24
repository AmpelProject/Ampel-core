from setuptools import setup
setup(name='Ampel-core',
      version='0.5.1',
      package_dir={'':'src'},
      packages=[
          'ampel.core',
          'ampel.core.abstract',
          'ampel.core.flags',
          'ampel.pipeline',
          'ampel.config',
          'ampel.config.channel',
          'ampel.config.t3',
          'ampel.config.time',
          'ampel.common',
          'ampel.db',
          'ampel.db.query',
          'ampel.logging',
          'ampel.t0',
          'ampel.t0.load',
          'ampel.t0.filter',
          'ampel.t0.ingest',
          'ampel.t2',
          'ampel.t3',
          'ampel.core.test',
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
              'ampel-followup = ampel.t0.DelayedT0Controller:run',
              'ampel-statspublisher = ampel.common.AmpelStatsPublisher:run',
              'ampel-exceptionpublisher = ampel.common.AmpelExceptionPublisher:run',
              'ampel-t2 = ampel.t2.T2Controller:run',
              'ampel-t3 = ampel.t3.T3Controller:main',
              'ampel-check-broker = ampel.t0.load.fetcherutils:list_kafka',
              'ampel-archive-topic = ampel.t0.load.fetcherutils:archive_topic',
          ],
          'ampel.resources' : [
              'mongo = ampel.common.resources:LiveMongoURI',
              'graphite = ampel.common.resources:Graphite',
              'slack = ampel.common.resources:SlackToken',
          ],
          'ampel.t0.units' : [
              'BasicFilter = ampel.t0.filter.BasicFilter:BasicFilter',
              'BasicMultiFilter = ampel.t0.filter.BasicMultiFilter:BasicMultiFilter'
          ],
          'ampel.t3.units' : [
              'T3PlaceboUnit = ampel.t3.T3PlaceboUnit:T3PlaceboUnit'
          ],
      }
)
