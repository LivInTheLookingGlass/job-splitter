from framework import run_jobs, _sleeper
from logging import basicConfig, DEBUG


if __name__ == '__main__':
    FORMAT = (
        "%(asctime)s - %(processName)s/%(threadName)s - %(filename)s:%(lineno)d (%(funcName)s) - "
        "%(name)s/%(levelname)s - %(message)s"
    )
    basicConfig(filename='test.log', level=DEBUG, format=FORMAT)
    run_jobs(_sleeper, [(x, ) for x in range(256)])
