"""Module that deals with all configuration related functions."""
from collections import OrderedDict
from configparser import ConfigParser, ExtendedInterpolation
from hashlib import sha3_512
from json import JSONDecoder
from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
from pathlib import Path
from pickle import dumps
from typing import TypeVar


T = TypeVar("T")

MACHINES_CONFIG = Path('machines.json')
CSV_HEADER = Path('header.csv')
RUNNER_CONFIG = Path('runner.config')

CONFIGS = [
    MACHINES_CONFIG,
    CSV_HEADER,
    RUNNER_CONFIG,
]
DEFAULT_CONFIG_PATH = Path(__file__).parent.joinpath('defaults', 'runner.config')
RESULTS_CSV = Path('results.csv')

CONFIG_TEMPLATES = [
    '{\n\t"example_machine": [example_one_core_weight, example_cores_used]\n}',
    '',
    DEFAULT_CONFIG_PATH.read_text()
]


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


def get_machines():  # -> OrderedDict[str, Tuple[int, int]]:
    """Read the list of machine names and their associated thread weights and number of threads."""
    decoder = JSONDecoder(object_pairs_hook=OrderedDict)
    with MACHINES_CONFIG.open('r') as f:
        return decoder.decode(f.read())


def parse_file_size(value: str) -> int:
    """Parse a string to return an integer file size."""
    value = value.lower()
    if value.endswith('t') or value.endswith('tib'):
        return int(value.rstrip('t').rstrip('tib')) << 40
    elif value.endswith('g') or value.endswith('gib'):
        return int(value.rstrip('g').rstrip('gib')) << 30
    elif value.endswith('m') or value.endswith('mib'):
        return int(value.rstrip('m').rstrip('mib')) << 20
    elif value.endswith('k') or value.endswith('kib'):
        return int(value.rstrip('k').rstrip('kib')) << 10
    elif value.endswith('tb'):
        return int(value[:2]) * 10**12
    elif value.endswith('gb'):
        return int(value[:2]) * 10**9
    elif value.endswith('mb'):
        return int(value[:2]) * 10**6
    elif value.endswith('kb'):
        return int(value[:2]) * 10**3
    return int(value)


def get_config() -> ConfigParser:
    """Read and parse the configuration file."""
    config = ConfigParser(interpolation=ExtendedInterpolation(), converters={'filesize': parse_file_size})
    with DEFAULT_CONFIG_PATH.open('r') as f:
        config.read_file(f)
    config.read(RUNNER_CONFIG)

    config['logging']['level'] = {
        'CRITICAL': str(CRITICAL),
        'ERROR': str(ERROR),
        'WARNING': str(WARNING),
        'INFO': str(INFO),
        'DEBUG': str(DEBUG),
        'NOTSET': str(NOTSET),
    }.get(config['logging']['level'], config['logging']['level'])

    return config
