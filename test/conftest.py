
import pytest
import pykafka
import subprocess
import time
import os
import tempfile

@pytest.fixture(scope="session")
def kafka_singularity_image():
    # check whether singularity exists
    try:
        subprocess.check_call(['singularity', '--version'], stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        pytest.skip("requires singularity")
    
    # pull the image for faster startup
    if 'SINGULARITY_CACHEDIR' not in os.environ:
        raise RuntimeError("SINGULARITY_CACHEDIR is not set in your environment. Set it to a directory on a large and fast filesystem.")
    image = os.path.join(os.environ['SINGULARITY_CACHEDIR'], "kafka.img")
    if not os.path.exists(image):
        subprocess.check_call(['singularity', 'pull', 'docker://spotify/kafka'], cwd=os.environ['SINGULARITY_CACHEDIR'])
    
    return image

def create_topic(name, partitions=1):
    subprocess.check_call(['singularity', 'exec', '--containall', '--cleanenv', kafka_singularity_image(), '/bin/sh', '-c', "$KAFKA_HOME/bin/kafka-topics.sh --topic '{}' --create --partitions {:d} --replication-factor 1 --zookeeper localhost:2181".format(name, partitions)])

@pytest.fixture(scope="session")
def kafka_server(kafka_singularity_image):
    """
    Start a kafka server on localhost from a Singularity container imported
    from DockerHub.
    """
  
    with tempfile.TemporaryDirectory() as tmpdir:
        # supervisord, zookeeper, and kafka all want to write to a bunch of
        # paths that are owned by root in the source Docker image. Bind them
        # to a temporary directory to make them writable by the current user.
        binds = []
        for path in ['/run', '/var/log/supervisor', '/var/log/zookeeper', '/var/log/kafka', '/var/lib/zookeeper']:
            binds += ['-B', tmpdir+':'+path]
        
        # let singularity itself see the environment, but clean it for the
        # contained process (except for the keys explicitly listed below)
        env=dict(ADVERTISED_HOST='localhost')
        env = {'SINGULARITYENV_'+k: v for k,v in env.items()}
        env.update(os.environ)
        
        proc = subprocess.Popen(['singularity', 'run', '--containall', '--cleanenv', '-W', tmpdir]+binds+[kafka_singularity_image], env=env)
        
        # wait for kafka to become available
        for i in range(30):
            try:
                client = pykafka.KafkaClient(hosts='localhost:9092')
            except pykafka.exceptions.NoBrokersAvailableError:
                time.sleep(1)
                continue
            break
        else:
            raise pykafka.exceptions.NoBrokersAvailableError
        
        create_topic('test', 1)
        
        try:
            yield
        finally:
            proc.terminate()
            proc.wait()
