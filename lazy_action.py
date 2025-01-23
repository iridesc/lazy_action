import time
import inspect
from diskcache import Cache
import functools

cache = Cache("./files/cache")


def lazy_action(expire=None):
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


@lazy_action(expire=5)
def test(t):
    print(f"!{t}")
    time.sleep(t)
    return time.time()


if __name__ == "__main__":
    test(1)
    test(2)
    test(1)
