
Deploying a new Ampel version
*****************************

The number of steps involved in deploying a new Ampel version depend on how much has changed from the configuration currently in production. In the following, I assume that you have set up `ssh` tunneling and Kerberos forwarding such that e.g. `ssh transit.ifh.de` dumps you into a shell on `transit.zeuthen.desy.de` without having to issue any more commands or enter a password. The top-level sections below describe the steps in the order they should be peformed, along with the conditions that make each necessary. If these conditions do not apply, you can skip the step and re-use the products of the previous deployment, e.g. the image or live database.

.. note:: Currently all the custom components of Ampel run on `burst`, so updates only involve restarting services there. The exception are upgrades to the avro alert schema stored in the archive database, which require operations on `transit` only.

Test, test, and test again
==========================

Testing on the production system should be avoided unless absolutely necessary. Make sure the unit tests pass, and write unit tests for every new feature you've added since the last release. This will save pain and anguish in the long run.

Build and push a new Ampel image
================================

Necessary if any of the following apply:

- You have added a new Ampel contrib project (e.g. ampel-contrib-foo).
- You have added external dependencies to an existing project
- You have added or changed console_scripts entrypoints in an existing project.

Set up GitHub repos to be pulled into the image
***********************************************

Currently all Ampel repositories are private. To pull them in an automated
fashion, you need to set up deploy keys that give read permission without
needing to type in your password. For each repository you wish to include,
change to `deploy/docker-images/devel/deploy-keys` and generate a key pair::
  
  ssh-keygen -t rsa -b 4096 -f ./id_rsa-$NAME

where `$NAME` is a short name associated with the repository, e.g. `ampel-contrib-hu`. For reasons that are `complicated and irritating<https://eclipsesource.com/blogs/2012/07/30/accessing-multiple-private-github-repositories-without-a-dedicated-build-user/>`_, the `$NAME` must match the fake domain name used in the `git clone` lines in the `Dockerfile`. Make an unencrypted key, i.e. one with no password. Point your web browser to the deploy keys page of the repository, e.g. https://github.com/AmpelProject/Ampel-core/settings/keys/new, and paste the contents of `.id_rsa-$NAME.pub` into the form.

Now, for each new repository you wish to add to the image, add a `git clone` line to the Dockerfile, replacing `github.com` with `$NAME`. Also add a pair of lines to the `pip` section of `environment.yml` to `pip install -e` the new repository at build time.

Build and tag the image
***********************

Change to `deploy/docker-images/devel` and `docker build -t ampel .`. Grab a coffee or 3 while the image builds.

Push image to Ampel machines
****************************

To transfer the image to `burst` and `transit`, you need to first publish the image to a Docker registry. Since we just got finished jumping through a series of hoops to build with private GitHub repositories, it seems silly to now turn around and push the resulting image to the public Docker Hub. Luckily, though, we can just run a private instance of the Docker registry. Start it like so::
  
  docker run -d -p 5000:5000 --restart=always --name registry registry:2

If you have dome this before, you may get an error message telling you that the name `registry` is already occupied. That's fine; you just want to ensure that the registry is running. Now, tag the Ampel image and push it to the registry::
  
  docker tag ampel localhost:5000/ampel:v0.3.5
  docker push localhost:5000/ampel:v0.3.5

Now, pull the image from the remote side::
  
  ssh -R5000:localhost:5000 transit.ifh.de
  sudo su ampel
  cd /data/ampel/singularity
  SINGULARITY_NOHTTPS=1 singularity pull docker://localhost:5000/ampel:v0.3.5

Initialize writable Ampel source directories
********************************************

The read-only Singularity image created in the first step contains clones of the Ampel project repositories. At this stage of development, though, we often need to iterate more quickly than would be practical if we had to build a new image for every single-line change. To that end, the current production configuration mounts a copy of the Ampel repos from the host filesystem over those in the image. To work correctly, however, these have to have the correct names and metadata. The easiest way to ensure this is to copy them directly out of the image _as you normal user_ with e.g.::
  
  singularity-stack volume init /data/ampel/singularity/ampel_v0.3.5.simg /Ampel $AMPEL_SRC
where `$AMPEL_SRC`  is the path where you'd like to store the source, e.g. in my case `/scratch/jvs/Ampel_v3`.

You should take care that any changes you make to these sources are tagged and
pushed to GitHub so that a different real user can easily take over as operator.

Prepare a new live database
===========================

Necessary if:

- You have changed the structure of the database that the Ampel core expects
- The filters and T2s have changed enough that a fresh start is warranted

1. On `burst`, become user `ampel` with `sudo su ampel`
2. Create a directory named after the version of Ampel-core you will run, e.g. `mkdir /data/ampel/mongo/live/v0.3.5`
3. Adjust `MONGO_PATH` in `$AMPEL_SRC/ampel-core/deploy/production/dryrun.env` to match
4. Set a new `GROUP_NAME` following the same convention. This will cause the AlertProcessors to re-ingest all alerts currently in the Kafka stream to the new database rather than starting where the old version left off.

Commit, tag, and push local changes
===================================

In the step above, you might have made changes the host copies of the Ampel repositories. Put these in a reproducible state by commiting the changes, tagging them appropriately, and pushing the tags to GitHub.

Obtain secrets
==============

Ampel and its plugins manage credentials for local and remote resources, e.g. read or write access to the live database, archive database, or push access to desyCloud. These credentials are not distributed with the source, but kept in read-protected files in a directory called `secrets`, currently in `/home/ampel/dryrun/secrets`. If any new ones have entered the mix, copy them here, making sure that they are owned and readable only by user Ampel.

Deploy Ampel
============

1. On `burst`, change to the directory containing the `secrets` subdirectory, currently `/home/ampel/dryrun`.
2. Become user `ampel` with `sudo su ampel`
3. Redeploy Ampel with `$AMPEL_SRC/ampel-core/deploy/up dryrun`. You should see output like the following::
  
  (singularity-stack) [burst] /home/ampel/dryrun > /scratch/jvs/Ampel-v0.3/ampel-core/deploy/up dryrun
  Stopping 39cf18b4 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=247505)
  Stopping 992d0449 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=247322)
  Stopping e49305fc instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245195)
  Stopping ba08f91e instance of /data/ampel/singularity/mongo-3.6.simg (PID=245010)
  Stopping f8b8ce10 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=247150)
  Stopping af186630 instance of /data/ampel/singularity/mongo-3.6.simg (PID=244898)
  Stopping a9404f1e instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245276)
  Stopping a03a6a85 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245365)
  Stopping 3dfcfabb instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245454)
  Stopping 1a40fc58 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245546)
  Stopping 15116b11 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245645)
  Stopping 18ee48b0 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245755)
  Stopping 90cd2739 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245859)
  Stopping 97b8d0c0 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=245972)
  Stopping db9e6bc3 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246086)
  Stopping 7e5e6d7c instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246203)
  Stopping c22ef49f instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246324)
  Stopping a87f8253 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246449)
  Stopping 3e8914d5 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246575)
  Stopping f1652941 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246712)
  Stopping eac9a825 instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246851)
  Stopping 36535acb instance of /data/ampel/singularity/ampel-v0.3.0.simg (PID=246992)
  not clearing anything
  connection refused on burst.zeuthen.desy.de:27018, retry after 1 s
  connection refused on burst.zeuthen.desy.de:27018, retry after 2 s
  connected to burst.zeuthen.desy.de:27018
  connection refused on burst.zeuthen.desy.de:27017, retry after 1 s
  connection refused on burst.zeuthen.desy.de:27017, retry after 2 s
  connected to burst.zeuthen.desy.de:27017
  Stack                          Services                       Replicas Instance
  ============================== ============================== ======= ========
  burst                          alertprocessor                 16      a9404f1e
                                 catalog                                af186630
                                 followup                               f8b8ce10
                                 mongo                                  ba08f91e
                                 stats                                  e49305fc
                                 t2-controller                          992d0449
                                 t3-controller                          39cf18b4
  ------------------------------ ------------------------------ ------- --------

Check logs for health
=====================

Monitor the logs of each of the services shown in the previous step to make sure the stack came up cleanly, without spewing errors all over the place. For example, to `tail` the logs of the `alertprocessor` service::
  
  singularity-stack logs burst alertprocessor -f

Note that since there are multiple replicas, lines of output may appear to be repeated.

