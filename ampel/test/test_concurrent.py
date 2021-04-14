import asyncio
import os
import random
import signal
import socket
import time
from contextlib import asynccontextmanager

import pytest
from prometheus_client.openmetrics.exposition import generate_latest

from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.metrics.AmpelProcessCollector import AmpelProcessCollector
from ampel.metrics.prometheus import mmap_dict
from ampel.util.concurrent import _Process, process


def echo(arg):
    time.sleep(random.uniform(0.0005, 0.001))
    return arg


def abort():
    signal.raise_signal(signal.SIGABRT)


def throw():
    raise NotImplementedError


class Unpicklable:
    def __reduce__(self):
        raise NotImplementedError


def return_unpicklable():
    return Unpicklable()


@process
def decorated(arg):
    return arg


@process(timeout=0.1)
def sleepy(dt, stubborn=False):
    if stubborn:
        signal.signal(signal.SIGTERM, lambda stack, n: 0)
    time.sleep(dt)
    return dt


@pytest.mark.asyncio
async def test_launch():
    p = _Process(target=echo, args=(42,))
    result = await p.launch()
    assert result == 42


@pytest.mark.asyncio
async def test_abort():
    p = _Process(target=abort)
    with pytest.raises(RuntimeError):
        await p.launch()


@pytest.mark.asyncio
async def test_raise():
    p = _Process(target=throw)
    with pytest.raises(NotImplementedError):
        await p.launch()


@pytest.mark.asyncio
async def test_too_many_arguments():
    p = _Process(target=throw, args=("borkybork",))
    with pytest.raises(TypeError):
        await p.launch()


@pytest.mark.asyncio
async def test_unpicklable_return():
    p = _Process(target=return_unpicklable)
    with pytest.raises(NotImplementedError) as excinfo:
        await p.launch()


@pytest.mark.asyncio
async def test_decorator():
    assert (await decorated(42)) == 42


@pytest.mark.asyncio
async def test_cancel():
    task = sleepy(5)
    await asyncio.sleep(1)
    task.cancel()
    with pytest.raises(asyncio.exceptions.CancelledError):
        await task


@pytest.mark.asyncio
async def test_kill():
    task = sleepy(10, stubborn=True)
    await asyncio.sleep(1)
    task.cancel()
    t0 = time.time()
    with pytest.raises(asyncio.exceptions.CancelledError):
        await task
    dt = time.time() - t0
    assert dt < 1, "task killed before exiting"


@pytest.mark.asyncio
async def test_multilaunch():
    launch = lambda: _Process(target=echo, args=(42,)).launch()
    count = 0
    num_tasks = 5
    pending = {launch() for _ in range(num_tasks)}
    done = {}
    while True:
        done, pending = await asyncio.wait_for(
            asyncio.wait(pending, return_when="FIRST_COMPLETED"), timeout=num_tasks
        )
        for future in done:
            assert future.result() == 42
            count += 1
        if count >= 20:
            for result in await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True), timeout=num_tasks
            ):
                assert result == 42
            break
        else:
            while len(pending) < num_tasks:
                pending.add(launch())


@process
def set_counter(value, process=None):
    AmpelMetricsRegistry.counter(
        "countcount", "cookies", subsystem="test_concurrent"
    ).inc(value)
    AmpelMetricsRegistry.histogram(
        "counthist", "cookies", subsystem="test_concurrent"
    ).observe(1)
    return value


@pytest.fixture
def prometheus_multiproc_dir(monkeypatch, tmpdir):
    """
    Ensure that Prometheus multiprocessing mode is enabled
    """
    from ampel.metrics.AmpelMetricsRegistry import MultiProcessCollector

    monkeypatch.setenv("PROMETHEUS_MULTIPROC_DIR", str(tmpdir))
    r = AmpelMetricsRegistry.registry()
    try:
        c = MultiProcessCollector(r)
        yield tmpdir
        r.unregister(c)
    except:
        yield


@pytest.mark.asyncio
async def test_multiprocess_metrics(prometheus_multiproc_dir):

    sample = lambda: {
        sample.name: sample.value
        for metric in AmpelMetricsRegistry.collect()
        for sample in metric.samples
        if sample
    }
    read_mmap = lambda fname: {
        k: v
        for k, v, p in mmap_dict.MmapedDict.read_all_values_from_file(
            prometheus_multiproc_dir / fname
        )
    }
    before = sample()

    value = 1100101
    assert (await set_counter(value, {"name": "0"})) == value

    assert sorted(os.listdir(prometheus_multiproc_dir)) == [
        "counter_archive.db",
        "histogram_archive.db",
    ], "persistent metrics were consolidated"

    after = sample()
    key = "ampel_test_concurrent_countcount_total"
    assert after[key] - before.get(key, 0) == value, "counter was incremented"

    hist_before = read_mmap("histogram_archive.db")
    await set_counter(value, {"name": "0"})
    hist_after = read_mmap("histogram_archive.db")
    assert len(hist_before) == len(
        hist_after
    ), "mmap files have constant size (if labels, including the implicit process label, are constant!)"
    for k in hist_before:
        assert hist_after[k] == hist_before[k] or hist_after[k] == 2 * hist_before[k]


@pytest.mark.asyncio
async def test_multiprocess_metrics_deduplication(prometheus_multiproc_dir):
    """
    Metrics are not duplicated in multiprocess mode
    """

    def get_help_lines():
        expo = generate_latest(AmpelMetricsRegistry)
        helps = []
        for line in expo.split(b"\n"):
            if line.startswith(b"# HELP"):
                helps.append(line.split(b" ")[2])
        return helps

    await set_counter(42, {"name": "0"})
    # register metric again in main process
    AmpelMetricsRegistry.counter("countcount", "cookies", subsystem="test_concurrent")

    helps = get_help_lines()
    assert len(set(helps)) == len(helps), "no duplicated HELP lines"


@pytest.mark.asyncio
async def test_implicit_labels(prometheus_multiproc_dir):
    """
    Implicit labels are added if there is a dict argument with a key "name"
    """

    get_sample_value = AmpelMetricsRegistry.registry().get_sample_value
    assert (
        get_sample_value("ampel_test_concurrent_countcount_total", {"name": "hola"})
        is None
    )

    value = 1100101
    assert (await set_counter(value, {"name": "hola"})) == value

    before = {}
    for metric in AmpelMetricsRegistry.registry().collect():
        for sample in metric.samples:
            key = (sample.name, tuple(sample.labels.items()))
            before[key] = sample.value

    assert (
        get_sample_value("ampel_test_concurrent_countcount_total", {"process": "hola"})
        is not None
    )


@asynccontextmanager
async def fence(port):
    """
    Wait for a connection on a TCP port, yield, then reply
    """
    condition = asyncio.Condition()
    server = None

    async def serve():
        nonlocal server

        async def connected(reader, writer):
            async with condition:
                condition.notify()
            async with condition:
                await condition.wait()
            writer.write(b"hola")
            await writer.drain()
            async with condition:
                condition.notify()

        server = await asyncio.start_server(connected, "127.0.0.1", port)
        return await server.serve_forever()

    serve = asyncio.create_task(serve())

    async with condition:
        await condition.wait()
    yield
    async with condition:
        condition.notify()
    async with condition:
        await condition.wait()
    server.close()
    try:
        await serve
    except asyncio.CancelledError:
        ...


def latch(port):
    """Connect to a port and wait for a reply before exiting"""
    sock = socket.create_connection(("127.0.0.1", port))
    sock.send(b"hola")
    sock.recv(4)


@pytest.mark.asyncio
async def test_process_collector(unused_tcp_port):
    for metric in AmpelProcessCollector().collect():
        assert not metric.samples

    for metric in AmpelProcessCollector(name="self").collect():
        assert len(metric.samples) == 1
        metric = metric.samples[0]
        assert metric.labels == {"process": "self", "replica": "0"}
        assert metric.value > 0

    proc = asyncio.create_task(
        _Process(latch, args=(unused_tcp_port,), name="latch").launch()
    )

    async with fence(unused_tcp_port):
        for metric in AmpelProcessCollector().collect():
            assert len(metric.samples) == 1
            metric = metric.samples[0]
            assert metric.labels == {"process": "latch", "replica": "0"}
            assert metric.value > 0
    await asyncio.wait_for(proc, 3)


@pytest.mark.asyncio
async def test_replica_numbering():
    launch = lambda: _Process(target=echo, args=(42,), name="echo").launch()
    assert not _Process._active
    assert not _Process._expired
    await launch()
    assert not _Process._active
    assert not _Process._expired
