
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

Test env on transit
*******************

# setup, as user jvsanten:

mkdir /scratch/jvs; cd /scratch/jvs
./Miniconda3-latest-Linux-x86_64.sh -p /scratch/jvs/miniconda3
echo . /scratch/jvs/miniconda3/etc/profile.d/conda.sh >> ~/.bashrc
. ~/.bashrc
git clone git@github.com:AmpelProject/singularity-stack.git
conda create --yes -n singularity-stack --file singularity-stack/requirements.txt python=3
conda activate singularity-stack
pip install -e singularity-stack

# running, as user ampel

# once at setup time
sudo su ampel
echo . /scratch/jvs/miniconda3/etc/profile.d/conda.sh >> ~/.bashrc
echo export SINGULARITY_CACHEDIR=/data/ampel/singularity >> ~/.bashrc
. ~/.bashrc

# create passwords for test deployment
mkdir -p $HOME/ampel-test/secrets; cd $HOME/ampel-test
echo mongo > secrets/mongo-root-password.txt
echo postgres > secrets/archive-user-password.txt
chmod og-rwx secrets/*.txt

singularity-stack -c /scratch/jvs/Ampel/deploy/test/docker-compose.yml deploy atest; singularity-stack -c /scratch/jvs/Ampel/deploy/test/docker-compose.yml logs -s ampel atest

# at every login
conda activate singularity-stack

sometimes we see:
  pkg_resources.DistributionNotFound: The 'ampel' distribution was not found and is required by the application
This means that Ampel/src/ampel.egg-info is missing in the source. pip install -e Ampel to create it.