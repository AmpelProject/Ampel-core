
import pytest
import asyncio
import time
import random
import signal

from ampel.util.concurrent import _Process, RemoteException

def echo(arg):
    time.sleep(random.uniform(0.0005,0.001))
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
async def test_multilaunch():
    launch = lambda: _Process(target=echo, args=(42,)).launch()
    count = 0
    num_tasks = 5
    pending = {launch() for _ in range(num_tasks)}
    done = {}
    while True:
        done, pending = await asyncio.wait_for(
            asyncio.wait(pending, return_when="FIRST_COMPLETED"),
            timeout=num_tasks
        )
        for future in done:
            assert future.result() == 42
            count += 1
        if count >= 20:
            for result in await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=num_tasks
            ):
                assert result == 42
            break
        else:
            while len(pending) < num_tasks:
                pending.add(launch())