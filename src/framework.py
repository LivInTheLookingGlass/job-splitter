from collections import OrderedDict
from configparser import ConfigParser
from copy import copy
from csv import writer
from dis import distb
from hashlib import sha3_512
from io import StringIO
from json import JSONDecoder
from logging import getLogger
from os import cpu_count
from pathlib import Path
from pickle import dumps
from random import Random, random
from subprocess import run
from time import sleep, time
from threading import Thread
from typing import Any, Callable, Iterable, Optional, Sequence, TypeVar, cast

from multiprocessing_logging import install_mp_handler
from progress import ProgressPool, Style

try:
    from os import nice
except ImportError:
    pass

# TODO: make able to distribute via PyInstaller if necessary
# TODO: default log template
# TODO: log zipper

T = TypeVar("T")

print(__file__)
input()

CONFIGS = [
    MACHINES_CONFIG := Path('machines.json'),
    CSV_HEADER := Path('header.csv'),
    RUNNER_CONFIG := Path('runner.config'),
    LOG_TEMPLATE := Path('logging.template'),
]
DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('defaults', 'runner.config')
RESULTS_CSV = Path('results.csv')

CONFIG_TEMPLATES = [
    '{\n\t"example_machine": [example_one_core_weight, example_cores_used]\n}',
    '',
    DEFAULT_CONFIG_PATH.read_text(),
    ''  # log_template
]


def _dummy(*args, **kwargs):
    pass


def get_entropy(*extras) -> bytes:
    """Generate bytes for use as a seed.

    Include additional objects only if they will be consistent from machine to machine.
    """
    hash_obj = sha3_512()
    for name in CONFIGS:
        hash_obj.update(name.read_bytes())
    hash_obj.update(dumps(extras))
    return hash_obj.digest()


def make_config_files():
    """Generate config files if not present, raising an error if any were not present."""
    any_triggered = False
    for name, default in zip(CONFIGS, CONFIG_TEMPLATES):
        try:
            with name.open('x') as f:
                f.write(default)
        except FileExistsError:
            pass
        else:
            any_triggered = True
    if any_triggered:
        raise FileNotFoundError("Your configuration files have been created. Please fill them out.")


def get_machines() -> OrderedDict[str, tuple[int, int]]:
    """Read the list of machine names and their associated thread weights and number of threads."""
    decoder = JSONDecoder(object_pairs_hook=OrderedDict)
    with MACHINES_CONFIG.open('r') as f:
        return decoder.decode(f.read())


def get_config() -> ConfigParser:
    """Read and parse the configuration file."""
    config = ConfigParser()
    with DEFAULT_CONFIG_PATH.open('r') as f:
        config.read_file(f)
    config.read(RUNNER_CONFIG)
    return config


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


def _sleeper(id_, *args, **kwargs):
    sleep(random() * 10)
    getLogger('JobRunner').info('done %i', id_)


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
    random_obj.shuffle(working_set)

    if ID is not None:
        START = sum(weights[:ID]) * lw // TOTAL
        STOP = sum(weights[:ID + 1]) * lw // TOTAL
        working_set = working_set[START:STOP]
    else:
        START = 0
    working_set = [(*items, copy(random_obj), config) for items in working_set]
    response = ''

    while not response.lower().startswith('y'):
        response = input((
            'I am {0}. Please verify this is correct.  Checksum: {1}.\n'
            '{2} jobs now queued ({3}-{4}). Total size {5}. (y/n)? '
        ).format("N/A" if ID is None else names[ID], entropy.hex(), len(working_set), START, STOP - 1, lw))
        if response.lower().startswith('n'):
            exit(1)

    if ID is None:
        num_cores = cpu_count()
    else:
        num_cores = machines[names[ID]][1]
    logger.info('%i jobs now queued (%i-%i). Total size %i', len(working_set), START, STOP - 1, lw)
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
        for idx, result in enumerate(p.istarmap_unordered(
            job,
            working_set,
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
            with RESULTS_CSV.open('a') as f:
                results_writer = writer(f)
                results_writer.writerow((start_time, new_time - last_time, new_time, *result))
            parse_function(config, start_time, new_time - last_time, new_time, *result)
            last_time = new_time
    return cast(T, current)
