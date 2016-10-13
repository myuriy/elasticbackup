# coding: utf-8
import logging

logging.basicConfig(format='%(asctime)s [%(name)s] [%(levelname)s] '
                           '%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
log = logging.getLogger('elasticbackup')
log_es = logging.getLogger('elasticsearch')


def positive_int(value):
    intval = int(value)
    if intval <= 0:
        raise ValueError("%s is an invalid positive int value" % value)
    return intval


def nonnegative_float(value):
    floatval = float(value)
    if floatval < 0:
        raise ValueError("%s is an invalid non-negative float value" % value)
    return floatval