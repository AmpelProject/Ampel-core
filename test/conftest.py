
import pytest
import pykafka
import subprocess
import time
import os

def singularity_run(image, env=dict()):
    env = {'SINGULARITYENV_'+k: v for k,v in env.items()}
    env.update(os.environ)
    try:
        return subprocess.Popen(['singularity', 'run', '--cleanenv', '--contain', image], env=env, preexec_fn=os.setsid)
    except FileNotFoundError:
        pytest.skip("requires singularity")

def feed_alerts():
    # wait for singularity to start up
    for i in range(10):
        try:
            client = pykafka.KafkaClient(hosts='localhost:9092')
        except pykafka.exceptions.NoBrokersAvailableError:
            time.sleep(5)
            continue
        break
    with client.topics[b'test'].get_sync_producer() as producer:
        for i in range(4):
            msg = 'test message {}'.format(i)
            producer.produce(msg.encode('utf-8'))
            

@pytest.fixture(scope="session")
def kafka_server():
    proc = singularity_run('docker://spotify/kafka', env=dict(ADVERTISED_HOST='localhost'))
    try:
        feed_alerts()
        yield
    finally:
        proc.terminate()
        proc.wait()
        # killing the start-kafka script does not kill the kafka server
        # reliably. remove any lingering java with extreme prejudice.
        subprocess.call(['killall', '-9', 'java'])
    