from copy import copy
from csv import writer
from dis import distb
from functools import partial
from gzip import open as open_gzip
from io import StringIO
from logging import getLogger
from os import cpu_count
from random import Random, random
from subprocess import run
from time import sleep, time
from threading import Thread
from typing import Any, Callable, Iterable, Optional, Sequence, cast

from multiprocessing_logging import install_mp_handler

from .config import CSV_HEADER, RESULTS_CSV, T, get_config, get_entropy, get_machines, make_config_files
from .progress import ProgressPool, Style

try:
    from os import nice
except ImportError:
    pass


def _dummy(*args, **kwargs):
    pass


def renicer_thread(pool):
    """Ensure that this process, and all its children, do not hog resources."""
    try:
        while True:
            nice(19)
            for process in pool._pool:
                try:
                    run(['renice', '-n', '19', str(process.pid)], capture_output=True)
                except Exception:
                    break
            sleep(3)
    except NameError:
        pass


def _sleeper(id_, *args, progress=None, **kwargs):
    size = int(random() * 10) + 1
    for idx in range(size):
        progress.report(idx, base=size)
        sleep(1)
    getLogger('JobRunner').info('done %i', id_)


def _indexed_job(job, index, given_args, *args, **kwargs):
    return (index, job(*given_args, *args, **kwargs))


def run_jobs(
    job: Callable,
    working_set: Iterable,
    setup_function: Callable = _dummy,
    setupargs: Sequence[Any] = (),
    parse_function: Callable = _dummy,
    process_initializer: Callable = _dummy,
    initargs: Sequence[Any] = (),
    reduce_function: Callable[[T, Any], T] = (lambda x, _: x),
    reduce_start: T = None,
    override_seed: Optional[bytes] = None
) -> T:
    logger = getLogger('JobRunner')
    logger.debug('Checking configuration files')
    make_config_files()

    machines = get_machines()
    names = tuple(machines)
    weights = tuple(thread_weight * threads for thread_weight, threads in machines.values())
    logger.debug('Loaded machines %r', machines)
    config = get_config()

    dialog = "\n".join((
        "Which node am I?",
        *("{}:\t{}".format(idx, name) for idx, name in enumerate(names)),
        ""
    ))

    while True:
        resp = input(dialog)
        if resp == 'N/A':
            logger.info('Instantiated without an ID. All jobs will be run.')
            ID = None
        else:
            try:
                ID = int(resp)
                logger.info('Instantiated as ID %i', ID)
            except Exception:
                continue
        break

    TOTAL = sum(weights)

    working_set = list(working_set)
    lw = STOP = len(working_set)

    entropy = get_entropy(working_set)
    logger.debug('Entropy: %s', entropy.hex())
    seed = override_seed or entropy
    if seed is override_seed:
        logger.info('Using override seed for random module: %r', seed)
    random_obj = Random(seed)
    indexed_set = [(idx, (*items, copy(random_obj), config)) for idx, items in enumerate(working_set)]
    random_obj.shuffle(indexed_set)

    if ID is not None:
        START = sum(weights[:ID]) * lw // TOTAL
        STOP = sum(weights[:ID + 1]) * lw // TOTAL
        indexed_set = indexed_set[START:STOP]
    else:
        START = 0
    response = ''

    while not response.lower().startswith('y'):
        response = input((
            'I am {0}. Please verify this is correct.  Checksum: {1}.\n'
            '{2} jobs now queued ({3}-{4}). Total size {5}. (y/n)? '
        ).format("N/A" if ID is None else names[ID], entropy.hex(), len(indexed_set), START, STOP - 1, lw))
        if response.lower().startswith('n'):
            exit(1)

    if ID is None:
        num_cores = cpu_count()
    else:
        num_cores = machines[names[ID]][1]
    logger.info('%i jobs now queued (%i-%i). Total size %i', len(indexed_set), START, STOP - 1, lw)
    logger.info(f"This machine will use {num_cores} worker cores")

    setup_function(config, *setupargs)

    start_time = last_time = time()
    current = reduce_start

    with RESULTS_CSV.open('w') as f:
        f.write(CSV_HEADER.read_text() + '\n')

    install_mp_handler()
    with ProgressPool(num_cores, initializer=process_initializer, initargs=initargs) as p:
        renicer = Thread(target=renicer_thread, args=(p, ), daemon=True)
        renicer.start()
        for idx, (job_id, result) in enumerate(p.istarmap_unordered(
            partial(_indexed_job, job),
            indexed_set,
            chunksize=int(config['ProgressPool']['chunksize']),
            bar_length=int(config['ProgressPool']['bar_length']),
            style=Style(int(config['ProgressPool']['style'])),
        ), start=1):
            logger.info("Answer received: %i/%i (%0.2f%%)", idx, len(working_set), 100.0 * idx / len(working_set))
            try:
                current = reduce_function(cast(T, current), result)
            except Exception:
                logger.exception("HEY! Your reduce function messed up!")
                try:
                    buff = StringIO()
                    distb(file=buff)
                    buff.seek(0)
                    logger.error(buff.read())
                except Exception:  # might fail in a C module?
                    pass
            new_time = time()
            if result is None:
                result = ()
            if config.getboolean('results', 'compress'):

                def open_method(name, mode='rt'):
                    return open_gzip(str(name) + '.gz', mode)

            else:
                open_method = open

            with open_method(config['results']['file_name'], mode='at') as f:
                results_writer = writer(f)
                meta = []
                if config.getboolean('results', 'include_start_time'):
                    meta.append(start_time)
                if config.getboolean('results', 'include_job_interval'):
                    meta.append(new_time - last_time)
                if config.getboolean('results', 'include_job_done_time'):
                    meta.append(new_time)
                if config.getboolean('results', 'include_job_id'):
                    meta.append(job_id)
                results_writer.writerow((*meta, *result))
            parse_function(config, *meta, *result)
            last_time = new_time
    return cast(T, current)
