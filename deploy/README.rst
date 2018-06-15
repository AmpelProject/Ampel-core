
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

# optional: copy source out of container as alternate user, e.g.:
singularity-stack volume init /data/ampel/singularity/ampel-v0.2a1.simg /Ampel /scratch/jvs/Ampel
# reset remote to real URL (since we've going to use our personal user's private key)
cd /scratch/jvs/Ampel/ampel-core && git remote set-url origin git@github.com:AmpelProject/Ampel.git

singularity-stack -c /scratch/jvs/Ampel/deploy/test/docker-compose.yml deploy atest; singularity-stack -c /scratch/jvs/Ampel/deploy/test/docker-compose.yml logs -s ampel atest

# at every login
conda activate singularity-stack

sometimes we see:
  pkg_resources.DistributionNotFound: The 'ampel' distribution was not found and is required by the application
This means that Ampel/src/ampel.egg-info (created by pip install -e) is missing in the source.
Copy it from the image to the source, e.g:
singularity mount /data/ampel/singularity/ampel-v0.1a7.simg cp -r /var/singularity/mnt/final/Ampel/src/ampel.egg-info src/

Tunneling around the firewall between burst and transit
*******************************************************

ztf-wgs can connect to burst and transit, but burst and transit can't connect
to each other on any but a few ports. To work around this, you can
reverse-tunnel traffic through ztf-wgs. For example, to connect to transit:5432
from burst, do this from ztf-wgs:
ssh burst -N4 -R5432:localhost:5433&
ssh transit -N4 -L5433:localhost:5432&

Rsyncing archive tarballs from Lustre to transit
************************************************

# from ice-wgs1, forward a port back from transit to allow outbound ssh connections
ssh -o "ProxyCommand ssh -qax ztf-wgs 'nc %h %p'" -R5000:localhost:22 transit

# rsync as local user ampel, but remote user jvsanten
sudo su ampel
(singularity-stack) [transit] /data/ampel/archive/tarballs > rsync -avz --progress -e 'ssh -p 5000' --include 'ztf_*programid*.tar.gz' --exclude '*' jvsanten@localhost:/lustre/fs19/group/icecube/jvs/ztf/ .

(singularity-stack) [burst] /data/ampel/catalogs > rsync -avz --progress -e 'ssh -p 5000' --include '**/' --include '**/GAIA/DR2/**' --exclude '*' jvsanten@localhost:/lustre/fs19/group/cta/users/mgiomi/catsHTM2 .

Resetting graphite aggregation and retention
********************************************

# go-graphite implements the collector and query API, but does not include whisperfile
# manipulation utilities. luckily, the official graphite-statsd does.
singularity shell -B /home/ampel/graphite-metrics:/mnt -C /data/ampel/singularity/graphite-statsd-1.0.2-2.sim
find /mnt/whisper -wholename '*Ampel-transit*/*.wsp' | xargs -n1 -I{} /usr/local/bin/whisper-set-xfilesfactor.py {} 0.01
find /mnt/whisper -wholename '*Ampel-transit*/*.wsp' | xargs -n1 -I{} /usr/local/bin/whisper-set-aggregation-method.py {} average

Recovering a Grafana dashboard without running Grafana
******************************************************

sqlite3 grafana/grafana.db 'select data from dashboard where title="Ampel burst Dashboard" > dashboard.json

Archive db backup and restore
*****************************
singularity shell -B /data/ampel/archive/dryrun.bak:/mnt $SINGULARITY_CACHEDIR/postgres-10.3.simg
pg_dump ztfarchive -a --format directory -j32 -f /mnt/ztfarchive --host localhost --port 5432 --username postgres --password