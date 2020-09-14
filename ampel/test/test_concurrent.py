
import pytest
import asyncio
import time
import random

from ampel.util.concurrent import _Process

def echo(arg):
    time.sleep(random.uniform(0.0005,0.001))
    return arg

@pytest.mark.asyncio
async def test_launch():
    p = _Process(target=echo, args=(42,))
    result = await p.launch()
    assert result == 42


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