# job-splitter
Package research code and split it among several machines in parallel

This package allows you to configure batched jobs to run across different machines.

## Goals

This project sets out with the following goals, of which we have currently met 1-5, 8, and 10-11 (8 of 11) complete:

1. Allow map()/reduce() representable, long-term compute jobs to be spread across multiple systems and cores
2. Distributed randomly (but consistently) across hosts
3. Proportionally to their compute ability
4. With detailed progress reports and per-host logs
5. With automatic data capturing
6. And snapshots to resume from (at least at the scale of "don't run a job twice")
7. With optional per-process/per-job logs
8. With optional all-unified logs, or at least a log merging script
9. With optional dynamic load balancing
10. With the ability to run on hosts you can't install things to
11. And wide configurability

## Features

- Dynamic load balancing between cores
- Automatic parallelization of isolated jobs
- Unified logging across all of a host's given processes
- Automatic compression of logs
- A configuration file automatically loaded into your method
- Automatic result compiling
- Detailed progress reporting on main console
- Distributable on machines without Python (conditional on PyInstaller working for your environment)
- Manual load balancing between machines

## Tutorial

### Generating Configuration Files

To begin, create your configuration files by running the `example_main.py` file in the src directory. This gives you:

1. A general purpose configuration file, passed to all your jobs
2. A CSV header file, which you should fill out (with the first entries as "Run Start Time", "Job Running Time", and "Job Start Time")
3. A machines configuration file

These configuration files need to be the same on all devices you will distribute to, or you need to provide an override seed for the distributed random objects. If these files are not identical it will affect job distribution.

### Filling out your machines file

The machines file is JSON formatted and expects a dictionary. Each key is a machine name, and each value is a pair consisting of its relative performance in a single thread, followed by its number of cores.

If this file is not filled out, you will not be able to start your jobs. These values do not need to be precise.

### Packaging for your first job

To package this for distributing to multiple machines, we make use of PyInstaller. We have already provided a Makefile which will assist in this. If you do not do anything unusual, and you do not need to package additional data files, this Makefile is sufficient for most needs.

First, modify the example_main.py file such that it calls your set of jobs. As this makes heavy use of the `map()` API, jobs must be phrased in a way where this will work.

Second, keep in mind that this package will inject arguments into your jobs. Essentially it modifies the signature of your method as follows:

```python
def job(..., random_obj, config, *, progress=<ProgressReporter>):
   ...
```

The random object is provided by the random module, and is initialized to the same seed for every job. If you need consistent randomness, this is your best bet. Do keep in mind that we do not guarantee that different versions of Python will produce the same sequence of outputs (and therefore that they would have consistent job distribution).

The configuration object is provided by the configparser module, and can largely be thought of as a two-layer dictionary. The first layer is the section name, the second the configuration name, and the resulit is the configuration value (as a string). The ConfigParser object also provides getters which do the type conversion for you for `bool`, `int`, `float`, and a custom converter we added for `filesize` which interprets size suffixes (ex: 64MiB).

The ProgressReporter object is provided by this package. Using its `.report()` method will allow you to report how far along the job you are, to be displayed in the main console. If you do not wish this to be out of 100, provide a custom base value.

### Distributing Across Systems

To build for distribution, call `make exe ENTRY=<your_entry>.py` in the main directory, substituting `<your_entry>` for the script you wish to build from. Note that this will only package for the OS/architecture version you are presently on. It is strongly advised that you build on the oldest version you plan to support.

If the Makefile provided is insufficient for your project, it at least gives you a place to start. For including additional files or imports that are not discovered automatically, please see the PyInstaller docs

Copy the resulting one-file program to each machine, as well as the related configuration files. When run on the new host, it will unpack your Python code and associated data, then run the program.

### Running the Program

When you start, it will ask you two questions. First, which machine is it? The machine you choose will then determine your share of the job pool, as well as how many cores to allocate.

After this, it will shuffle the job pool to ensure that each node gets a random distribution of tasks. That way if any node goes down, it affects all job groupings roughly evenly.

Because this needs to be done deterministically, the random number seed is generated using your job pool and configuration files as entropy. This means that you ABSOLUTELY MUST check the hash digest displayed on your screen.
