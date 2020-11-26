import asyncio
import random
import signal
import time

import pytest

from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
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
def set_counter(value):
    AmpelMetricsRegistry.counter("countcount", "cookies").inc(value)
    return value


@pytest.fixture
def enable_multiproc(monkeypatch, tmpdir):
    """
    Ensure that Prometheus multiprocessing mode is enabled
    """
    from ampel.metrics.AmpelMetricsRegistry import MultiProcessCollector
    monkeypatch.setenv("prometheus_multiproc_dir", str(tmpdir))
    r = AmpelMetricsRegistry.registry()
    try:
        c = MultiProcessCollector(r)
        yield
        r.unregister(c)
    except:
        yield

@pytest.mark.asyncio
async def test_multiprocess_metrics(enable_multiproc):

    sample = lambda: {sample.name: sample.value for metric in AmpelMetricsRegistry.collect() for sample in metric.samples if sample}
    before = sample()

    value = 1100101
    assert (await set_counter(value)) == value

    after = sample()
    key = "ampel_test_concurrent_countcount_total"
    print(after)
    assert after[key] - before.get(key, 0) == value
