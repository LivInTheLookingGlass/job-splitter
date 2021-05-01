from logging import getLogger, Formatter
from logging.handlers import RotatingFileHandler
from typing import Type

from src.framework import get_config, make_config_files, run_jobs, _sleeper
from src.zipped_logs import ZippedRotatingFileHandler


if __name__ == '__main__':
    # setup section
    make_config_files()
    config = get_config()

    formatter = Formatter(config['logging']['format'] % ())
    log = getLogger()
    log.setLevel(config.getint('logging', 'level'))
    file_name = config['logging']['file_name']
    if config.getboolean('logging', 'auto_zip'):
        fh_type: Type[RotatingFileHandler] = ZippedRotatingFileHandler
    else:
        fh_type = RotatingFileHandler
    fh = fh_type(
        file_name + '.txt',
        mode='w',
        encoding='utf-8',
        maxBytes=config.getfilesize('logging', 'max_file_size'),  # type: ignore # pylint: disable=no-member
        backupCount=config.getfilesize('logging', 'backup_count'),  # type: ignore # pylint: disable=no-member
    )

    fh.setLevel(config.getint('logging', 'level'))
    fh.setFormatter(formatter)
    log.addHandler(fh)

    # setup done
    try:
        run_jobs(_sleeper, [(x, ) for x in range(256)])
        # optional teardown
        # ...
    finally:
        # required teardown
        log.removeHandler(fh)
        fh.flush()
        fh.close()
