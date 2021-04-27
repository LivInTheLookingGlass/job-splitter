# job-splitter
Package research code and split it among several machines in parallel

This package allows you to configure batched jobs to run across different machines.

# How To Use

## Running Once

To begin, create your configuration files by running the `example_main.py` file in the src directory. This gives you:

1. A general purpose configuration file, passed to all your jobs
2. A CSV header file, which you should fill out (with the first entries as "Run Start Time", "Job Running Time", and "Job Start Time")
3. A machines configuration file

These configuration files need to be the same on all devices you will distribute to, or you need to provide an override seed for the distributed random objects. If these files are not identical it will affect job distribution.

## Filling out your machines file

The machines file is JSON formatted and expects a dictionary. Each key is a machine name, and each value is a pair consisting of its relative performance in a single thread, followed by its number of cores.

If this file is not filled out, you will not be able to start your jobs. These values do not need to be precise.

## Packaging for your first job

To package this for distributing to multiple machines, we make use of PyInstaller. We have already provided a Makefile which will assist in this. If you do not do anything unusual, and you do not need to package additional data files, this Makefile is sufficient for most needs.

First, modify the example_main.py file such that it calls your set of jobs. As this makes heavy use of the `map()` API, jobs must be phrased in a way where this will work.

Second, keep in mind that this package will inject arguments into your jobs. Essentially it modifies the signature of your method as follows:

```python
def job(..., random_obj, config, *, progress=<ProgressReporter>):
   ...
```

The random object is provided by the random module, and is initialized to the same seed for every job. If you need consistent randomness, this is your best bet. Do keep in mind that we do not guarantee that different versions of Python will produce the same sequence of outputs.

The configuration object is provided by the configparser module, and can largely be thought of as a two-layer dictionary. The first layer is the section name, the second the configuration name, and the third the configuration value (as a string).

The ProgressReporter object is provided by this package. Calling its .get() method will allow you to report how far along the job you are, to be displayed in the main console. If you do not wish this to be out of 100, provide a custom base value.
