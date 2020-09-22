Installing
----------

Ampel is written in Python 3.6, and split into multiple packages (see :ref:`projects` for a full list). Of these, you should only need ampel-base and an instrument-specific package like ampel-ztf.

There are a number of ways to install Ampel for local development and testing. We recommend :ref:`conda_dedicated`.

.. _conda_dedicated:

Option 1: Dedicated `conda` environment
=======================================

1. `Install miniconda <https://conda.io/miniconda.html>`_.

2. Download the :download:`environment definition file </_static/ampel-environment.yml>`, displayed below. Replace XXX with the Ampel repository password, available upon request. You may also change the name of the environment (ampel-dev by default) if you so wish.

.. literalinclude:: /_static/ampel-environment.yml

3. Create the environment: `conda env create -f ampel-environment.yml`

4. Activate the environment: `source activate ampel-dev`.

If you later add packages (or new dependencies to support your contribution), add them to the environment file and `conda env update -f ampel-environment.yml`.

To use your contribution with the core Ampel packages, run `pip install -e .` in its source directory. This links your package source into the environment's search path, making e.g. `ampel.contrib.yourgroup.t2.YourT2Unit` importable.

Option 2: Add to existing `conda` environment
=============================================

1. Activate your target conda environment.

2. Add some custom channels to search for packages::
    
    for channel in conda-forge ampelproject https://ampel:XXX@www-zeuthen.desy.de/~jvsanten/ampel/conda; do
      conda config --env --add channels $channel
    done

3. Install Ampel packages, e.g. `conda install ampel-ztf`, and `pip install` your own contributions as described above.

Option 3: Custom Python environment
===================================

If you have strong opinions about how your system is set up, you can also install all Ampel components by hand. We recommend, however, that you at least use virtualenv to isolate yourself from your base OS Python install. You will need at least Python 3.6. 

Running the full chain
======================

To run the full Ampel processing chain implemented in the ampel-core project, you will need MongoDB (>= 3.6). It is easily run via Docker, or you may install it locally.

