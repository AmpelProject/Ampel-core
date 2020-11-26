#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/concurrent.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 07.08.2020
# Last Modified Date: 07.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

"""
Simple decorator for running a function in subprocess with asyncio, adapted
from pebble.concurrent.process. Like pebble, the future returned by the
decorated function can be cancelled to terminate the underlying subprocess.
Unlike pebble (or concurrent.futures.ProcessPoolExecutor), no extra Python
threads are needed to manage the process lifecycle.
"""


import asyncio
import io
import itertools
import os
import signal
import sys
import traceback
from textwrap import dedent
from typing import Any
from functools import wraps, partial
from multiprocessing import reduction, spawn  # type: ignore
from multiprocessing.context import set_spawning_popen
from subprocess import _args_from_interpreter_flags  # type: ignore

import aiopipe  # type: ignore
from prometheus_client.multiprocess import mark_process_dead # type: ignore


def process(function=None, **kwargs):
    """
    Runs the decorated function in a concurrent process. All arguments and
    return values must be pickleable.

    The decorated function will return an asyncio.Task that can be awaited in
    an event loop. The task will complete when the function returns or raises
    an exception. If the task is cancelled, the process will be terminated.
    """
    if function is None:
        return partial(_process_wrapper, **kwargs)
    else:
        return _process_wrapper(function)


class RemoteTraceback(Exception):
    """Traceback wrapper for exceptions in remote process.

    Exception.__cause__ requires a BaseException subclass.

    """

    def __init__(self, traceback):
        self.traceback = traceback

    def __str__(self):
        return self.traceback


class RemoteException(BaseException):
    """Pickling wrapper for exceptions in remote process."""

    def __init__(self, exception, traceback):
        self.exception = exception
        self.traceback = traceback

    def __reduce__(self):
        return rebuild_exception, (self.exception, self.traceback)


def rebuild_exception(exception, traceback):
    exception.__cause__ = RemoteTraceback(traceback)

    return exception


def prepare(data):
    "stripped-down version of multiprocessing.spawn.prepare()"
    if "sys_path" in data:
        sys.path = data["sys_path"]

    if "sys_argv" in data:
        sys.argv = data["sys_argv"]

    if "dir" in data:
        os.chdir(data["dir"])

    if "init_main_from_name" in data:
        spawn._fixup_main_from_name(data["init_main_from_name"])
    elif "init_main_from_path" in data:
        spawn._fixup_main_from_path(data["init_main_from_path"])


def spawn_main(read_fd, write_fd):
    """
    Execute pickled _Process received over pipe
    """
    with open(read_fd, "rb") as rx:
        preparation_data = reduction.pickle.load(rx)
        prepare(preparation_data)
        obj = reduction.pickle.load(rx)
    ret = None
    exitcode = 1
    try:
        ret = obj()
        exitcode = 0
        payload = reduction.pickle.dumps(ret)
    except Exception as error:
        error.traceback = traceback.format_exc()
        ret = RemoteException(error, error.traceback)
        payload = reduction.pickle.dumps(ret)
    try:
        with open(write_fd, "wb") as tx:
            tx.write(payload)
    except:
        print(f"Process {obj._name} (pid {os.getpid()}):", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        exitcode = 1

    sys.exit(exitcode)


class _Process:
    _counter = itertools.count(1)

    def __init__(self, target=None, name=None, timeout=3.0, args=(), kwargs={}):
        self._target = target
        count = next(self._counter)
        self._name = name if name else f"{count}"
        self._timeout = timeout
        self._args = tuple(args)
        self._kwargs = dict(kwargs)

    def __call__(self):
        if self._target:
            return self._target(*self._args, **self._kwargs)

    def _get_command_line(self, read_fd, write_fd):
        return (
            spawn.get_executable(),
            *_args_from_interpreter_flags(),
            "-c",
            f"from ampel.util.concurrent import spawn_main; spawn_main({read_fd}, {write_fd})",
        )

    async def launch(self) -> Any:
        prep_data = spawn.get_preparation_data(self._name)
        fp = io.BytesIO()
        set_spawning_popen(self)
        try:
            reduction.dump(prep_data, fp)
            reduction.dump(self, fp)
        finally:
            set_spawning_popen(None)

        parent_r, child_w = aiopipe.aiopipe()
        child_r, parent_w = aiopipe.aiopipe()

        with child_r.detach() as crx, child_w.detach() as ctx:
            proc = await asyncio.subprocess.create_subprocess_exec(
                *self._get_command_line(crx._fd, ctx._fd),
                pass_fds=sorted(p._fd for p in (crx, ctx)),
                start_new_session=True,
            )

        try:
            async with parent_w.open() as tx:
                tx.write(fp.getbuffer())
                await tx.drain()

            async with parent_r.open() as rx:
                try:
                    exitcode, payload = await asyncio.gather(
                        proc.wait(), rx.read(), return_exceptions=True
                    )
                    if isinstance(exitcode, BaseException):
                        raise exitcode
                    elif exitcode < 0:
                        signame = signal.Signals(-exitcode).name
                        raise RuntimeError(f"Process {self._name} (pid {proc.pid}) died on {signame}")
                    if isinstance(payload, BaseException):
                        raise payload
                    else:
                        ret = reduction.pickle.loads(payload)
                    if isinstance(ret, BaseException):
                        raise ret
                    else:
                        return ret
                except asyncio.CancelledError:
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), self._timeout)
                    except asyncio.TimeoutError:
                        proc.kill()
                    await asyncio.gather(proc.wait(), rx.read())
                    raise
        finally:
            if "prometheus_multiproc_dir" in os.environ:
                mark_process_dead(proc.pid)


_registered_functions = {}


def _register_function(function):
    global _registered_functions

    _registered_functions[(function.__qualname__, function.__module__)] = function


def _trampoline(name, module, *args, **kwargs):
    """Trampoline function for decorators.

    Lookups the function between the registered ones;
    if not found, forces its registering and then executes it.

    """
    function = _function_lookup(name, module)

    return function(*args, **kwargs)


def _function_lookup(name, module):
    """Searches the function between the registered ones.
    If not found, it imports the module forcing its registration.

    """
    if module == "__main__":
        module = "__mp_main__"
    try:
        return _registered_functions[(name, module)]
    except KeyError:  # force function registering
        __import__(module)
        return _registered_functions[(name, module)]


def _process_wrapper(function, timeout=3.0):
    # keep the wrapped function so we can actually call it
    _register_function(function)

    @wraps(function)
    def wrapper(*args, **kwargs):
        target = _trampoline
        args = [function.__qualname__, function.__module__] + list(args)
        proc = _Process(target=target, timeout=timeout, args=args, kwargs=kwargs)
        return asyncio.create_task(proc.launch())

    return wrapper
