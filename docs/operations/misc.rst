
Odds and ends
*************

Reset Kafka consumer group offsets
==================================

The AlertProcessors are fed alerts by a Kafka consumer. These consumers
identify themselves to the broker by a group name (in Ampel, e.g.
ampel-vX.Y.Z-public), and the brokers distributes each message exactly once to
exactly one consumer in the group. This means that once an AlertProcessor has
consumed an alert, you will never see it again. If a situation arises where you
need to replay the alerts from a particular topic, you can reset these using
the command-line tools that ship with Kafka.

1. Shut down all consumers in the group.

Perform the following steps on transit (only ztf-wgs, transit, and burst are
whitelisted on the UW Kafka server):

2. Get a Kafka image: `singularity pull docker://wurstmeister/kafka:2.11-2.0.1`
3. Check the offsets for the group and topic you want to reset, in this case topic `ztf_20181115_programid2` and group `ampel-v0.4.0-partnership`::
	
	singularity exec --contain kafka-2.11-2.0.1.simg /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server epyc.astro.washington.edu:9092 --group ampel-v0.4.0-partnership --describe | awk 'NR<3 || /ztf_20181115/'

	TOPIC                   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                  HOST            CLIENT-ID
	ztf_20181115_programid2 6          915             44166           43251           rdkafka-8b6414d5-e95b-47ef-91ed-1b5c52fb9d56 /172.18.0.1     rdkafka
	ztf_20181115_programid1 6          879             13303           12424           rdkafka-8b6414d5-e95b-47ef-91ed-1b5c52fb9d56 /172.18.0.1     rdkafka
	ztf_20181115_programid2 8          782             44165           43383           rdkafka-bac4e41d-8950-4175-af7e-22e6711a508a /172.18.0.1     rdkafka
	ztf_20181115_programid1 8          764             13304           12540           rdkafka-bac4e41d-8950-4175-af7e-22e6711a508a /172.18.0.1     rdkafka
	ztf_20181115_programid2 3          803             44166           43363           rdkafka-6a94c6ad-a874-40e7-844b-579b09cde9cc /172.18.0.1     rdkafka
	ztf_20181115_programid1 3          776             13303           12527           rdkafka-6a94c6ad-a874-40e7-844b-579b09cde9cc /172.18.0.1     rdkafka
	ztf_20181115_programid1 5          708             13304           12596           rdkafka-6e2d9175-92c0-4938-810a-72980c8a8f27 /172.18.0.1     rdkafka
	ztf_20181115_programid2 5          741             44166           43425           rdkafka-6e2d9175-92c0-4938-810a-72980c8a8f27 /172.18.0.1     rdkafka
	ztf_20181115_programid1 13         780             13304           12524           rdkafka-d3eb71b7-34a4-4710-96f8-0f51358be6e3 /172.18.0.1     rdkafka
	ztf_20181115_programid2 13         805             44166           43361           rdkafka-d3eb71b7-34a4-4710-96f8-0f51358be6e3 /172.18.0.1     rdkafka
	ztf_20181115_programid2 15         828             44166           43338           rdkafka-f05cf880-c7f7-42fc-95f8-9e59149cc1d3 /172.18.0.1     rdkafka
	ztf_20181115_programid1 15         791             13303           12512           rdkafka-f05cf880-c7f7-42fc-95f8-9e59149cc1d3 /172.18.0.1     rdkafka
	ztf_20181115_programid2 2          737             44166           43429           rdkafka-54dd569d-2a2a-4726-9e21-fb2ba1a60d51 /172.18.0.1     rdkafka
	ztf_20181115_programid1 2          707             13304           12597           rdkafka-54dd569d-2a2a-4726-9e21-fb2ba1a60d51 /172.18.0.1     rdkafka
	ztf_20181115_programid1 10         825             13303           12478           rdkafka-c00dee2a-225c-4875-9259-5811fa28a915 /172.18.0.1     rdkafka
	ztf_20181115_programid2 10         863             44166           43303           rdkafka-c00dee2a-225c-4875-9259-5811fa28a915 /172.18.0.1     rdkafka
	ztf_20181115_programid1 7          821             13303           12482           rdkafka-ba76dd04-3592-4df0-97bf-fb3d4c453d62 /172.18.0.1     rdkafka
	ztf_20181115_programid2 7          849             44166           43317           rdkafka-ba76dd04-3592-4df0-97bf-fb3d4c453d62 /172.18.0.1     rdkafka
	ztf_20181115_programid2 12         832             44166           43334           rdkafka-d155f375-a45a-49bc-af6b-411296018562 /172.18.0.1     rdkafka
	ztf_20181115_programid1 12         806             13303           12497           rdkafka-d155f375-a45a-49bc-af6b-411296018562 /172.18.0.1     rdkafka
	ztf_20181115_programid2 0          823             44165           43342           rdkafka-46acf77a-8d9a-4b21-b99e-e07b10fa7d2d /172.18.0.1     rdkafka
	ztf_20181115_programid1 0          796             13303           12507           rdkafka-46acf77a-8d9a-4b21-b99e-e07b10fa7d2d /172.18.0.1     rdkafka
	ztf_20181115_programid2 9          800             44166           43366           rdkafka-bb3f4a15-bb29-47f0-b366-2612533cb979 /172.18.0.1     rdkafka
	ztf_20181115_programid1 9          765             13303           12538           rdkafka-bb3f4a15-bb29-47f0-b366-2612533cb979 /172.18.0.1     rdkafka
	ztf_20181115_programid2 11         873             44165           43292           rdkafka-c42072ee-cbb4-4283-96b0-de0f2cf17de5 /172.18.0.1     rdkafka
	ztf_20181115_programid1 11         844             13304           12460           rdkafka-c42072ee-cbb4-4283-96b0-de0f2cf17de5 /172.18.0.1     rdkafka
	ztf_20181115_programid1 1          810             13303           12493           rdkafka-46b0e796-c65f-45b8-9383-9ae41705fca8 /172.18.0.1     rdkafka
	ztf_20181115_programid2 1          841             44166           43325           rdkafka-46b0e796-c65f-45b8-9383-9ae41705fca8 /172.18.0.1     rdkafka
	ztf_20181115_programid2 4          700             44166           43466           rdkafka-6a96a212-bd73-4741-b8f2-9ae356611d81 /172.18.0.1     rdkafka
	ztf_20181115_programid1 4          674             13304           12630           rdkafka-6a96a212-bd73-4741-b8f2-9ae356611d81 /172.18.0.1     rdkafka
	ztf_20181115_programid2 14         850             44166           43316           rdkafka-e2d54690-ecaa-4e38-a311-b92fdd699b62 /172.18.0.1     rdkafka
	ztf_20181115_programid1 14         822             13304           12482           rdkafka-e2d54690-ecaa-4e38-a311-b92fdd699b62 /172.18.0.1     rdkafka
4. Reset the offset to earliest::
	singularity exec --contain kafka-2.11-2.0.1.simg /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server epyc.astro.washington.edu:9092 --group ampel-v0.4.0-partnership --topic ztf_20181115_programid2 --reset-offsets --to-earliest --execute

	TOPIC                          PARTITION  NEW-OFFSET
	ztf_20181115_programid2        6          0
	ztf_20181115_programid2        14         0
	ztf_20181115_programid2        2          0
	ztf_20181115_programid2        8          0
	ztf_20181115_programid2        9          0
	ztf_20181115_programid2        11         0
	ztf_20181115_programid2        3          0
	ztf_20181115_programid2        15         0
	ztf_20181115_programid2        10         0
	ztf_20181115_programid2        0          0
	ztf_20181115_programid2        4          0
	ztf_20181115_programid2        12         0
	ztf_20181115_programid2        5          0
	ztf_20181115_programid2        1          0
	ztf_20181115_programid2        13         0
	ztf_20181115_programid2        7          0
5. Restart the consumers

Add a new extcats catalog
=========================

1. Punch a hole in the firewall for `rsync` from ztf-wgs::
    
    ssh -R5000:localhost:22 burst

2. Become user ampel and sync catalogs::
    
    sudo su ampel
    cd /data/ampel/catalogs
    rsync -avz --progress -e 'ssh -p 5000' jvsanten@localhost:/lustre/fs19/group/cta/users/mgiomi/mongodumps .

3. Restore catalogs and update roles::
    
    SINGULARITYENV_MONGO_INITDB_ROOT_PASSWORD=`cat ~/dryrun/secrets/mongo-root-password.txt` SINGULARITYENV_MONGO_INITDB_ROOT_USERNAME=root singularity-stack exec burst catalog /docker-entrypoint-initdb.d/add_catalog.sh $CATALOG_NAME
  
  where `CATALOG_NAME` is the name of the catalog you just rsynced.

Add a new catsHTM catalog
=========================

1. Punch a hole in the firewall for `rsync` from ztf-wgs::
    
    ssh -R5000:localhost:22 burst

2. Become user ampel and sync catalogs::
    
    sudo su ampel
    cd /data/ampel/catalogs
    rsync -avz --progress -e 'ssh -p 5000' jvsanten@localhost:/lustre/fs19/group/cta/users/mgiomi/catsHTM2 .

Export target catalog for cross-checks
======================================

On burst: (replace `af186630` with the container id of the catalog service, and `$PASSWORD` with the catalog read password)::
  
  singularity shell instance://af186630
  cd /docker-entrypoint-initdb.d/
  mongoexport --port 27018 --username filterclient --password $PASSWORD --authenticationDatabase admin --db ToO --collection neutrinos --jsonArray -o neutrinos.json