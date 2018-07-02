from distutils.core import setup
setup(name='ampel',
      version='0.2.0',
      package_dir={'':'src'},
      packages=['ampel'],
      entry_points = {
          'console_scripts' : [
              'ampel-alertprocessor = ampel.pipeline.t0.AlertProcessor:run_alertprocessor',
              'ampel-statspublisher = ampel.pipeline.common.AmpelStatsPublisher:run',
              'ampel-t2 = ampel.pipeline.t2.T2Controler:run',
              'ampel-t3 = ampel.pipeline.t3.T3Controller:run',
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
