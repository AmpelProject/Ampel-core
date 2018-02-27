from distutils.core import setup
setup(name='ampel',
      version='0.1',
      package_dir={'':'src'},
      packages=['ampel'],
      entry_points = {
          'console_scripts' : [
              'ampel-alertprocessor = ampel.pipeline.t0.AlertProcessor:run_alertprocessor',
              'ampel-init-db = ampel.pipeline.t0.AlertProcessor:init_db',
          ]
      }
)
