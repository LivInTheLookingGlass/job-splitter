#pragma once

#include <Python.h>
#include <unistd.h>

extern short update_progress(PyObject *p, double progress, double base);

struct ReturnValue {
    double value;
};

struct ReturnValue implemented_job(double sleep_time, PyObject *random, PyObject *config, PyObject *reporter)   {
    double i;
    for (i = 0; i < sleep_time; i++) {
        Py_BEGIN_ALLOW_THREADS  // release GIL
        sleep(1);
        Py_END_ALLOW_THREADS  // acquire GIL
        update_progress(reporter, i + 1, sleep_time);
    }
    return (struct ReturnValue){3};
}