"""Implements an extended version of the multiprocessing.Pool object to allow progress reporting.

Using several subclasses and wrapper objects, this module injects data into the standard multiprocessing pool. So long
as the API for this does not change substantially, this module should work, and does not need to be updated very
frequently.
"""

from collections import defaultdict
from contextlib import contextmanager
from enum import IntEnum
from functools import partial
from itertools import count
from multiprocessing import TimeoutError, get_context
from multiprocessing.pool import AsyncResult, IMapIterator, Pool, mapstar, starmapstar  # type: ignore
from time import sleep, time
from queue import SimpleQueue
from sys import stdout
from random import random
from threading import Thread
from typing import Any, Callable, Generic, Iterable, Iterator, Mapping, Optional, Sequence, TypeVar
from warnings import warn

try:
    from reprint import output
    output_not_present = False
except Exception:
    output_not_present = True

# TODO: docstrings
# TODO: tests
# TODO: custom bar format

_pool_id_gen = count()
_pool_queue_map: dict[int, SimpleQueue] = {}
_singleton_map: dict[str, 'Singleton'] = {}

_T = TypeVar("_T")
_U = TypeVar("_U")

is_atty = stdout.isatty()


@contextmanager
def nullcontext(enter_result=None):
    """Backport of contextlib.nullcontext for if output is not usable in a given environment."""
    yield enter_result


class Style(IntEnum):
    LOW_JOB_AND_TOTAL = 0
    ACTIVE_JOBS_AND_TOTAL = 1
    NON_TTY_SAFE = 2


class Singleton:
    """Stub class that ensures only one instance of an object is ever made.

    To be used for specific sentinel values.
    """

    def __new__(cls, name: str) -> 'Singleton':
        """Override __new__ such that it only spawns one instance."""
        if name not in _singleton_map:
            _singleton_map[name] = super().__new__(cls)
        return _singleton_map[name]

    def __init__(self, name: str):
        self.name = name


STOP_THREAD = Singleton("STOP_THREAD")


class ProgressReporter:
    """Reporter object injected into all calls made by the ProgressPool.

    Using this object, one can report incremental progress on their compute job back to the main process.
    """

    def __init__(self, q_id: int, job_id: int):
        self.q_id = q_id
        self.job_id = job_id

    def report(self, progress: float, base: float = 100.0):
        """Report a progress value back to the main process.

        By default, it is read as a float between 0 and 100, but the base can be overwritten on request.

        Raises
        ------
        ValueError: if not (0 <= progress <= base)
        """
        if not (0.0 <= progress <= base):
            raise ValueError("progress is outside valid range")
        if base != 100.0:
            progress = progress * 100.0 / base
        _pool_queue_map[self.q_id].put((self.job_id, progress))

    def done(self):
        """Report back to the main process that this task is done."""
        self.report(100)


def _initializer(
    q_id: int,
    queue: SimpleQueue,
    orig_init: Optional[Callable],
    initargs: Sequence[Any] = ()
):
    _pool_queue_map[q_id] = queue
    if orig_init is not None:
        return orig_init(*initargs)


def _wrap_prog(tup, star=False, **kwargs):
    func, q_id, arg, job_id = tup
    reporter = ProgressReporter(q_id, job_id)
    try:
        if star:
            ret = func(*arg, progress=reporter, **kwargs)
        else:
            ret = func(arg, progress=reporter, **kwargs)
        return ret
    finally:
        reporter.done()


class WrappedObject:
    def __init__(self, obj):
        self._wrapped_object = obj

    def __getattr__(self, name: str):
        if name in self.__dict__:
            return getattr(self, name)
        return getattr(self._wrapped_object, name)


class ProgressResult(WrappedObject):
    def __init__(self, result, id_range: Sequence[int]):
        super().__init__(result)
        self._pool: ProgressPool
        self.id_range = id_range
        self.current = min(id_range)

    def fix_style(self, style: Style) -> Style:
        if style != Style.NON_TTY_SAFE:
            if not is_atty:
                warn("Running in a non-tty environment, falling back to one line style", RuntimeWarning)
                return Style.NON_TTY_SAFE
            elif output_not_present:
                warn("Running without reprint library, falling back to one line style", RuntimeWarning)
                return Style.NON_TTY_SAFE
        return style

    def _handle_multiple(
        self,
        job_id: int,
        rel_id: int,
        max_len: int,
        style: Style,
        bar_length: int,
        fill_char: str,
        output_lines
    ) -> Any:
        if style == Style.LOW_JOB_AND_TOTAL:
            key = f'Job {str(rel_id).zfill(max_len)} Progress'
            prog = self._pool.get_progress(job_id)
            if prog >= 100.0:
                self.current += 1
                if key in output_lines:
                    del output_lines[key]
                return True
            cells = int(prog * bar_length / 100.0)
            output_lines[key] = "[{done}{padding}] {percent:2.02f}%".format(
                done=fill_char * cells,
                padding=" " * (bar_length - cells),
                percent=prog
            )
        elif style == Style.ACTIVE_JOBS_AND_TOTAL:
            known_done = set()
            for rel_id, job_id in enumerate(self.id_range):
                if job_id not in known_done and (prog := self._pool.get_progress(job_id)):
                    key = f'Job {str(rel_id).zfill(max_len)} Progress'
                    if prog >= 100.0:
                        known_done.add(job_id)
                        if key in output_lines:
                            del output_lines[key]
                    else:
                        cells = int(prog * bar_length / 100.0)
                        output_lines[key] = "[{done}{padding}] {percent:2.02f}%".format(
                            done=fill_char * cells,
                            padding=" " * (bar_length - cells),
                            percent=prog
                        )
        elif style == Style.NON_TTY_SAFE:
            t_progress = self._pool.get_progress(job_id)
            cells = int(t_progress * bar_length / 100.0)
            done: int = output_lines  # type: ignore
            print(
                "\rJob {id} Progress: [{done}{padding}] {percent:2.02f}% ({i}/{l})".format(
                    id=str(rel_id).zfill(max_len),
                    done=fill_char * cells,
                    padding=" " * (bar_length - cells),
                    percent=t_progress,
                    i=done,
                    l=len(self.id_range)
                ),
                end="\r"
            )

    def print_info(self, style: Style, bar_length: int, output_lines, max_len: int, fill_char: str = "#", main_name: str = 'Total Progress'):
        # style 0, where the lowest ID'd active job is tracked
        if style == 0:
            for rel_id, job_id in enumerate(self.id_range):
                if job_id != self.current:
                    continue
                if self._handle_multiple(job_id, rel_id, max_len, style, bar_length, fill_char, output_lines):
                    continue
                t_progress = sum(
                    self._pool.get_progress(job_id)
                    for job_id in self.id_range
                ) / len(self.id_range)
                cells = int(t_progress * bar_length / 100.0)
                output_lines[main_name] = "[{done}{padding}] {percent:2.02f}% ({i}/{l})".format(
                    done=fill_char * cells,
                    padding=" " * (bar_length - cells),
                    percent=t_progress,
                    i=rel_id,
                    l=len(self.id_range)
                )
        # style 1, where every active job is tracked, not just the lowest ID'd one
        elif style == 1:
            self._handle_multiple(0, 0, max_len, style, bar_length, fill_char, output_lines)
            t_progress = sum(self._pool.get_progress(job_id) for job_id in self.id_range) / len(self.id_range)
            cells = int(t_progress * bar_length / 100.0)
            output_lines[main_name] = "[{done}{padding}] {percent:2.02f}%".format(
                done=fill_char * cells,
                padding=" " * (bar_length - cells),
                percent=t_progress
            )
        # style 2, where total progress is given in a non-tty-safe manner
        elif style == 2:
            progs = [self._pool.get_progress(job_id) for job_id in self.id_range]
            done = sum(p == 100.0 for p in progs)
            t_progress = sum(progs) / len(self.id_range)
            cells = int(t_progress * bar_length / 100.0)
            length = len(self.id_range)
            if length <= 1 or (time() % 4) < 2:
                print(
                    "\r{main_name}: [{done}{padding}] {percent:2.02f}% ({i}/{l})".format(
                        main_name=main_name,
                        done=fill_char * cells,
                        padding=" " * (bar_length - cells),
                        percent=t_progress,
                        i=done,
                        l=length
                    ),
                    end="\r"
                )
            else:
                for rel_id, job_id in enumerate(self.id_range):
                    if self._pool.get_progress(job_id) == 100.0:
                        continue
                    break
                self._handle_multiple(job_id, rel_id, max_len, style, bar_length, fill_char, done)
        else:
            raise ValueError(style)


class ProgressImapResult(ProgressResult, Iterable[_T]):
    def __init__(
        self,
        result: IMapIterator,
        id_range: Sequence[int],
        style: Style,
        bar_length: int,
        fill_char: str,
        pool: 'ProgressPool',
        main_name: str = 'Total Progress'
    ):
        super().__init__(result, id_range)
        self.style = style
        self.bar_length = bar_length
        self._pool = pool
        self.fill_char = fill_char
        self.main_name = main_name

    def __iter__(self) -> Iterator[_T]:
        max_len = len(str(len(self.id_range) - 1))
        self.current = self.id_range[0]
        style = self.fix_style(self.style)
        context = output(output_type='dict') if style != Style.NON_TTY_SAFE else nullcontext({})
        with context as output_lines:
            while True:
                self.print_info(style, self.bar_length, output_lines, max_len, self.fill_char, self.main_name)
                try:
                    yield self.next(timeout=0.05)
                except TimeoutError:
                    pass
                except StopIteration:
                    break
        if style == 2:
            print()
        yield from self._wrapped_object


class ProgressMapResult(ProgressResult, Generic[_T]):
    def __init__(self, result: AsyncResult[_T], id_range: Sequence[int]):
        super().__init__(result, id_range)

    def get(
        self,
        timeout: float = None,
        style: Style = Style.ACTIVE_JOBS_AND_TOTAL,
        bar_length: int = 10,
        fill_char: str = "#",
        main_name: str = 'Total Progress'
    ):
        if timeout is None:
            limit = float('inf')
        else:
            limit = time() + timeout
        max_len = len(str(len(self.id_range) - 1))
        style = self.fix_style(style)
        context = output(output_type='dict') if style != Style.NON_TTY_SAFE else nullcontext({})
        with context as output_lines:
            while not self.ready() and time() < limit:
                self.print_info(style, bar_length, output_lines, max_len, fill_char, main_name)
                sleep(0.05)
        # if style == 2:
            # print()
        return self._wrapped_object.get(timeout=0)


class ProgressAsyncResult(ProgressMapResult, Generic[_T]):
    def _handle_multiple(self, *args, **kwargs) -> Any:
        pass


class ProgressPool(Pool):
    """Multiprocessing pool modified to keep track of job progress and print it to the console.

    Note
    ----
    Do not use print statements while you are using a call like map(), map_async().get(), or imap(), as it will mess
    with progress bar formatting. If you do so, it is advised that you should pad the end of the line with whitespace
    so the message is more clearly readable. Future versions will provide helper methods to make this easier.

    TODO: provide helper methods to ensure garbage is not produced when printing, probably by having a lock and a
    helper method to clear the line.
    """

    def __init__(
        self,
        processes: Optional[int] = None,
        initializer: Optional[Callable[[], None]] = None,
        initargs: Sequence[Any] = (),
        maxtasksperchild: Optional[int] = None,
        context=None
    ):
        context = context or get_context()
        self._pool_id = next(_pool_id_gen)
        self._prog_queue: SimpleQueue = context.SimpleQueue()
        _pool_queue_map[self._pool_id] = self._prog_queue
        self._id_generator = count()
        initializer = partial(_initializer, self._pool_id, self._prog_queue, initializer, initargs)
        self._active = True
        self._listener_thread = Thread(target=self._listen, daemon=True)
        self._listener_thread.start()
        self._start_job_id = 0
        self._end_job_id = 0
        self._progress_entries: Mapping[int, float] = defaultdict(int)
        super().__init__(processes, initializer, (), maxtasksperchild, context)

    def __del__(self):
        del _pool_queue_map[self._pool_id]
        super().__del__()

    def _listen(self):
        while self._active:
            tup = self._prog_queue.get()
            if tup is STOP_THREAD:
                return
            job_id, progress = tup
            if job_id > self._end_job_id:
                self._end_job_id = job_id
            self._progress_entries[job_id] = progress
            while self._progress_entries[self._start_job_id] >= 100.0:
                former = self._start_job_id
                self._start_job_id += 1
                del self._progress_entries[former]

    def get_progress(self, job_id: int) -> float:
        if job_id < self._start_job_id:
            return 100.0
        elif job_id > self._end_job_id:
            return 0.0
        return self._progress_entries[job_id]

    def close(self):
        self._prog_queue.put(STOP_THREAD)
        super().close()
        self._listener_thread.join()

    def apply(  # type: ignore[override]
        self,
        func: Callable[..., _T],
        args: Iterable[Any] = (),
        kwds: Optional[Mapping[str, Any]] = None,
        callback: Optional[Callable[[_T], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None,
        style: Style = Style.ACTIVE_JOBS_AND_TOTAL,
        bar_length: int = 10,
        main_name: str = 'Total Progress'
    ) -> _T:
        return self.apply_async(func, args, kwds, callback, error_callback).get(
            None,
            style,
            bar_length,
            main_name
        )

    def apply_async(  # type: ignore[override]
        self,
        func: Callable[..., _T],
        args: Iterable[Any] = (),
        kwds: Optional[Mapping[str, Any]] = None,
        callback: Optional[Callable[[_T], None]] = None,
        error_callback: Optional[Callable[[BaseException], None]] = None
    ) -> ProgressAsyncResult[_T]:
        if kwds is None:
            kwds = {}
        ids = (next(self._id_generator), )
        res = super().apply_async(
            partial(_wrap_prog, star=True),
            ((func, self._pool_id, args, ids[0]), ),
            kwds,
            callback,
            error_callback
        )
        return ProgressAsyncResult(res, ids)

    def map(  # type: ignore[override]
        self,
        func: Callable[[_U, ProgressReporter], _T],
        iterable: Iterable[_U],
        chunksize=None,
        style: Style = Style.ACTIVE_JOBS_AND_TOTAL,
        bar_length: int = 10,
        fill_char: str = "#",
        main_name: str = 'Total Progress'
    ) -> list[_T]:
        return self._map_async(
            func,
            iterable,
            mapstar,
            chunksize=chunksize
        ).get(
            style=style,
            bar_length=bar_length,
            fill_char=fill_char,
            main_name=main_name
        )

    def starmap(
        self,
        func: Callable[..., _T],
        iterable: Iterable[Iterable],
        chunksize=None,
        style: Style = Style.ACTIVE_JOBS_AND_TOTAL,
        bar_length: int = 10,
        fill_char: str = "#",
        main_name: str = 'Total Progress'
    ) -> list[_T]:
        return self._map_async(
            func,
            iterable,
            starmapstar,
            chunksize=chunksize
        ).get(
            style=style,
            bar_length=bar_length,
            fill_char=fill_char,
            main_name=main_name
        )

    def _map_async(
        self,
        func: Callable[..., _T],
        iterable,
        mapper,
        chunksize: Optional[int] = None,
        callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None
    ) -> ProgressMapResult[_T]:
        is_star = (mapper is starmapstar)
        jobs = tuple(iterable)
        ids = tuple(next(self._id_generator) for _ in jobs)
        res = super()._map_async(  # type: ignore
            partial(_wrap_prog, star=is_star),
            ((func, self._pool_id, t, j_id) for t, j_id in zip(jobs, ids)),
            mapstar,
            chunksize,
            callback,
            error_callback
        )
        return ProgressMapResult(res, ids)

    def imap(  # type: ignore[override]
        self,
        func: Callable[..., _T],
        iterable,
        *args,
        **kwargs
    ) -> ProgressImapResult[_T]:
        return self._imap(func, iterable, mapstar, ordered=True, **kwargs)

    def imap_unordered(  # type: ignore[override]
        self,
        func: Callable[..., _T],
        iterable,
        *args,
        **kwargs
    ) -> ProgressImapResult[_T]:
        return self._imap(func, iterable, mapstar, ordered=False, **kwargs)

    def istarmap(self, func: Callable[..., _T], iterable, *args, **kwargs) -> ProgressImapResult[_T]:
        return self._imap(func, iterable, starmapstar, ordered=True, **kwargs)

    def istarmap_unordered(self, func: Callable[..., _T], iterable, *args, **kwargs) -> ProgressImapResult[_T]:
        return self._imap(func, iterable, starmapstar, ordered=False, **kwargs)

    def _imap(
        self,
        func: Callable[..., _T],
        iterable,
        mapper,
        *args,
        ordered: bool = True,
        style: Style = Style.ACTIVE_JOBS_AND_TOTAL,
        bar_length: int = 10,
        fill_char: str = "#",
        main_name: str = 'Total Progress',
        **kwargs
    ) -> ProgressImapResult[_T]:
        is_star = (mapper is starmapstar)
        jobs = tuple(iterable)
        ids = tuple(next(self._id_generator) for _ in jobs)
        if ordered:
            iterator: IMapIterator = super().imap(
                partial(_wrap_prog, star=is_star),
                ((func, self._pool_id, t, j_id) for t, j_id in zip(jobs, ids)),
                *args,
                **kwargs
            )
        else:
            iterator = super().imap_unordered(
                partial(_wrap_prog, star=is_star),
                ((func, self._pool_id, t, j_id) for t, j_id in zip(jobs, ids)),
                *args,
                **kwargs
            )
        return ProgressImapResult(iterator, ids, style, bar_length, fill_char, self, main_name)


def demo_sleep(t: float, progress: ProgressReporter):
    """Sleep, but report progress back to the main process."""
    start = time()
    limit = start + (t or float('inf'))
    while (now := time()) < limit:
        progress.report((now - start) / t, base=1.0)
        sleep(1)


if __name__ == '__main__':
    with ProgressPool() as pool:
        print("Let's test it out for each method/style combo!")
        for name, style in Style.__members__.items():
            print()
            print(f"apply() style {name}")
            pool.apply_async(
                demo_sleep,
                (random() * 10, )
            ).get(
                style=style,
                bar_length=100
            )
        for map_method in (pool.map, pool.imap, pool.imap_unordered):
            for name, style in Style.__members__.items():
                print()
                print(f"{map_method.__name__}() style {name}")
                tuple(map_method(  # type: ignore
                    demo_sleep,
                    (random() * 10 for _ in range(10)),
                    style=style,
                    bar_length=100
                ))
        for starmap_method in (pool.starmap, pool.istarmap, pool.istarmap_unordered):
            for name, style in Style.__members__.items():
                print()
                print(f"{starmap_method.__name__}() style {name}")
                tuple(starmap_method(  # type: ignore
                    demo_sleep,
                    ((random() * 10, ) for _ in range(10)),
                    style=style,
                    bar_length=100
                ))
