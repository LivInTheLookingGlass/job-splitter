# cython: language_level=3

from time import sleep

cdef extern from "math.h":
    double NAN

cdef public api enum ErrorType:
    NO_ERROR = 0
    GEN_EXCEPTION = -1
    VALUE_ERROR = -2

cdef public api struct DoubleWithError:
    double value
    ErrorType error

cdef public api short update_progress(object p, double progress, double base) with gil:
    try:
        p.report(progress, base)
    except ValueError:
        return VALUE_ERROR
    except:
        return GEN_EXCEPTION
    return NO_ERROR

cdef inline (double, ErrorType) _increment_progress(object p, double progress, double base) with gil:
    try:
        return (p.increment(progress, base), NO_ERROR)
    except ValueError:
        return (NAN, VALUE_ERROR)
    except:
        return (NAN, GEN_EXCEPTION)

cdef public api DoubleWithError increment_progress(object p, double progress, double base):
    ret = _increment_progress(p, progress, base)
    return DoubleWithError(ret[0], ret[1])

cdef public void py_sleep(double sleep_time):
    sleep(sleep_time)

cdef extern from "/home/gappleto/Syncthing/MSU Notes/Research Framework/example_header.h":
    struct ReturnValue:
        double value

    ReturnValue implemented_job(double sleep_time, object random, object config, object reporter)

def job(sleep_time, random, config, progress):
    ret = implemented_job(sleep_time, random, config, progress)
    return (ret.value, )