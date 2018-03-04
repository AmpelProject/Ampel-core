
Deploying the Ampel stack
=========================

Ampel consists of a set of related services, all of which should be started and
stopped together. Each of these services is packaged in a container. The method
for deploying these varies depending on the container runtime.

Docker
******

.. note:: This has not actually been tested. YMMV.

1. Clone the Ampel git repository somewhere, and `cd` to `Ampel/deploy`.
2. Bring the stack up with `docker stack deploy ampel`
3. Find the id of the `ampel` container with `docker stack ls`
4. Watch the AlertProcessor logs for activity: `docker logs --follow ID`, where `ID` is the container identifier from step 3.
5. Shut down the stack with `docker stack rm ampel`

Singularity
***********

For Singularity we use `singularity-stack <https://github.com/AmpelProject/singularity-stack/>`_,
a poor man's version of the `docker stack` family of commands. This requires a
little more initial setup.

1. Install Singularity >= 2.4
2. Choose a location for your Singularity layer cache, and set the `SINGULARITY_CACHEDIR` environment variable. You will need about 10 GB of storage.
3. Install the self-contained package of `singularity-stack` from the `releases page <https://github.com/AmpelProject/singularity-stack/releases>`_.
4. Clone the Ampel git repository somewhere, and `cd` to `Ampel/deploy`.
5. Bring the stack up with `singularity-stack deploy ampel`
6. Watch the AlertProcessor logs for activity: `singularity-stack logs -s ampel ampel`
7. Shut down the stack with `singularity-stack rm ampel`