from loguru import logger
from functools import wraps


def log_mode():
    def logging_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            logger.debug("%s-%s begin call" % (func.__module__, func.__name__))
            result = func(*args, **kwargs)
            logger.debug("%s-%s end call with result-type: %s" % (func.__module__, func.__name__, type(result)))
            return result
        return wrapped_function
    return logging_decorator


class LogMode:

    def __init__(self, func):
        self.func = func

    def __call__(self, o, *args):
        logger.debug("%s begin call" % self.func.__name__)
        self.func(o, *args)
        logger.debug("%s begin end" % self.func.__name__)
