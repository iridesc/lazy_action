import inspect
from diskcache import Cache
import functools

lazy_action_cache = Cache(".lazy_action_cache")


def lazy_action(expire=None, cache=None):
    cache = cache if cache else lazy_action_cache
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (inspect.getabsfile(func), func.__name__, args, tuple(kwargs.items()))
            if key in cache:
                result = cache[key]
            else:
                result = func(*args, **kwargs,)
                cache.set(key, result, expire=expire)
            return result

        return wrapper

    return decorator


