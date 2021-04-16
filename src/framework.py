from collections import OrderedDict
from copy import copy
from csv import reader, writer
from dis import distb
from hashlib import md5, sha3_512
from json import JSONDecoder
from os import cpu_count
from pathlib import Path
from pickle import dumps as pickle_dumps
from random import Random
from subprocess import run
from time import sleep, time
from threading import Thread
from traceback import format_exc
from typing import Any, Callable, Iterable, Optional, Sequence, TypeVar, cast

from progress import ProgressPool

try:
    from os import nice
except ImportError:
    pass

# TODO: synchronized logging
# TODO: make able to distribute via PyInstaller if necessary
# TODO: default log template
# TODO: have configuration stuff
# TODO: log zipper

T = TypeVar("T")

CONFIGS = [
    MACHINES_CONFIG := Path('machines.json'),
    CSV_HEADER := Path('header.csv'),
    RUNNER_CONFIG := Path('runner.config'),
    LOG_TEMPLATE := Path('logging.template'),
]
RESULTS_CSV = Path('results.csv')

CONFIG_TEMPLATES = [
    '{\n\t"example_machine": [example_one_core_weight, example_cores_used]\n}',
    '',
    Path(__file__).parent.joinpath('default.runner.config').read_text(),
    ''  # log_template
]


def get_entropy(*extras) -> bytes:
    """Generate bytes for use as a seed.

    Include additional objects only if they will be consistent from machine to machine.
    """
    hash_obj = sha3_512()
    for name in CONFIGS:
        hash_obj.update(name.read_bytes())
    hash_obj.update(pickle_dumps(extras))
    return hash_obj.digest()


def make_config_files():
    """Generate config files if not present, raising an error if any were not present."""
    any_triggered = False
    for name, default in zip(CONFIGS, CONFIG_TEMPLATES):
        try:
            with name.open('x') as f:
                f.write(default)
        except FileExistsError:
            any_triggered = True
    if any_triggered:
        raise FileNotFoundError("Your configuration files have been created. Please fill them out.")


def get_machines() -> OrderedDict[str, tuple[int, int]]:
    """Read the list of machine names and their associated thread weights and number of threads."""
    decoder = JSONDecoder(object_pairs_hook=OrderedDict)
    with MACHINES_CONFIG.open('r') as f:
        return decoder.decode(f.read())


def get_config() -> dict[str, Any]:
    """Read and parse the configuration file."""
    config = {}
    with RUNNER_CONFIG.open('r') as f:
        config_reader = reader(f, delimiter=' ')
        for name, value in config_reader:
            config[name] = value
    return config


def renicer_thread(pool):
    """Ensure that this process, and all its children, do not hog resources."""
    try:
        while True:
            nice(19)
            for process in pool._processes:
                try:
                    run(['renice', '-n19', str(process.pid)])
                except Exception:
                    break
            sleep(1)
    except NameError:
        pass


def run_jobs(
    job: Callable,
    working_set: Iterable,
    setup_function: Optional[Callable] = None,
    setupargs: Sequence[Any] = (),
    process_function: Optional[Callable] = None,
    process_initializer: Optional[Callable] = None,
    initargs: Sequence[Any] = (),
    chunksize: int = 1,
    bar_length: int = 50,
    reduce_function: Optional[Callable[[T, Any], T]] = None,
    reduce_start: T = None,
    override_seed: Optional[bytes] = None
) -> T:
    make_config_files()

    machines = get_machines()
    names = tuple(machines)
    weights = tuple(thread_weight * threads for thread_weight, threads in machines.values())
    config = get_config()

    dialog = "\n".join((
        "Which node am I?",
        *("{}:\t{}".format(idx, name) for idx, name in enumerate(names)),
        ""
    ))

    while True:
        resp = input(dialog)
        if resp == 'N/A':
            ID = None
        else:
            try:
                ID = int(resp)
            except Exception:
                continue
        break

    TOTAL = sum(weights)

    working_set = list(working_set)
    lw = STOP = len(working_set)
    checksum = md5(pickle_dumps(working_set)).hexdigest().upper()

    seed = override_seed or get_entropy(working_set)
    random_obj = Random(seed)
    random_obj.shuffle(working_set)

    if ID is not None:
        START = sum(weights[:ID]) * lw // TOTAL
        STOP = sum(weights[:ID + 1]) * lw // TOTAL
        working_set = working_set[START:STOP]
    else:
        START = 0
    working_set = [(*items, copy(random_obj)) for items in working_set]
    response = ''

    while not response.lower().startswith('y'):
        response = input((
            'I am {0}. Please verify this is correct.  Checksum: {1}.\n'
            '{2} jobs now queued ({3}-{4}). Total size {5}. (y/n)? '
        ).format("N/A" if ID is None else names[ID], checksum, len(working_set), START, STOP - 1, lw))
        if response.lower().startswith('n'):
            exit(1)

    if ID is None:
        num_cores = cpu_count()
    else:
        num_cores = machines[names[ID]][1]
    print(f"This machine will use {num_cores} worker cores")

    if setup_function is not None:
        setup_function(config, *setupargs)

    start_time = last_time = time()
    current = reduce_start

    with ProgressPool(num_cores, initializer=process_initializer, initargs=initargs) as p:
        renicer = Thread(target=renicer_thread, args=(p, ), daemon=True)
        renicer.start()
        for result in p.istarmap_unordered(job, working_set, chunksize=chunksize, bar_length=bar_length):
            if reduce_function is not None:
                try:
                    current = reduce_function(cast(T, current), result)
                except Exception:
                    print("HEY! Your reduce function messed up!")
                    format_exc()
                    try:
                        distb()
                    except Exception:  # might fail in a C module?
                        pass
            new_time = time()
            if result is None:
                result = ()
            with RESULTS_CSV.open('w') as f:
                results_writer = writer(f)
                results_writer.writerow((start_time, new_time - last_time, new_time, *result))
            if process_function is not None:
                process_function(config, start_time, new_time - last_time, new_time, *result)
            last_time = new_time
    return cast(T, current)
