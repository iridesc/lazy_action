import inspect
import time
from diskcache import Cache
import functools
import shutil
from sqlite3 import DatabaseError
import os


lazy_action_folder = ".lazy_action_cache"
lazy_action_cache_path = f"{lazy_action_folder}/cache-{time.time()}"
log_prefix = "lazy_action >>"


def _del_path(path):
    prefix = f"{log_prefix} _del_path path={path}"
    if not os.path.exists(path):
        return

    if os.path.isdir(path):
        try:
            shutil.rmtree(path)

        except Exception as e:
            print(f"{prefix}  delete folder error:  {e}")
            for file_inside in os.listdir(path):
                _del_path(os.path.join(path, file_inside))

    else:
        try:
            os.remove(path)
        except Exception as e:
            print(f"{prefix} delete file error: {e}")


def _rm_caches():
    for name in os.listdir(lazy_action_folder):
        if name.startswith("cache-"):
            target_folder = os.path.join(lazy_action_folder, name)
            _del_path(target_folder)


def _reset_cache():
    global lazy_action_cache_path
    global lazy_action_cache

    if lazy_action_cache is not None:
        try:
            lazy_action_cache.close()  # 尝试关闭底层连接
        except Exception as close_e:
            print(f"{log_prefix} Error closing cache explicitly: {close_e}")
    lazy_action_cache = None

    if os.path.exists(lazy_action_folder):
        print(f"{log_prefix} cache folder exists")
        if os.path.isdir(lazy_action_folder):
            _rm_caches()
            lazy_action_cache_path = os.path.join(
                lazy_action_folder, f"cache-{time.time()}"
            )
        else:
            print(f"{log_prefix} cache folder exists but is not a directory, removing")
            os.remove(lazy_action_folder)
            os.mkdir(lazy_action_folder)
            lazy_action_cache_path = os.path.join(
                lazy_action_folder, f"cache-{time.time()}"
            )
    else:
        print(f"{log_prefix} create cache folder")
        os.mkdir(lazy_action_folder)
        lazy_action_cache_path = os.path.join(
            lazy_action_folder, f"cache-{time.time()}"
        )
    lazy_action_cache = Cache(lazy_action_cache_path)
    print(f"{log_prefix} cache reset to {lazy_action_cache_path}")


try:
    if not os.path.exists(lazy_action_folder):
        os.makedirs(lazy_action_folder)
    names = os.listdir(lazy_action_folder)
    names.sort(reverse=True)

    for name in names:
        if name.startswith("cache-"):
            lazy_action_cache_path = os.path.join(lazy_action_folder, name)
            break

    lazy_action_cache = Cache(lazy_action_cache_path)
except DatabaseError:
    print(f"{log_prefix} DatabaseError remove cache file")
    _reset_cache()

except Exception as e:
    print(f"{log_prefix} unknown error, reset cache! e={e}")
    _reset_cache()


def _get_or_run_and_set(
    key,
    func,
    args,
    kwargs,
    expire,
):
    is_in_cache = False
    try:
        is_in_cache = key in lazy_action_cache
    except Exception as e:
        print(f"{log_prefix} unknown error in check key in cache, reset cache! e={e}")
        _reset_cache()

    if is_in_cache:
        try:
            return lazy_action_cache[key]
        except Exception as e:
            print(
                f"{log_prefix} unknown error in lazy_action fetch result, reset cache! e={e}"
            )
            result = func(
                *args,
                **kwargs,
            )
            _reset_cache()
            lazy_action_cache.set(key, result, expire=expire)
            return result

    else:
        result = func(
            *args,
            **kwargs,
        )
        try:
            lazy_action_cache.set(key, result, expire=expire)
            return result
        except Exception as e:
            print(
                f"{log_prefix} unknown error in lazy_action set result reset cache! e={e}"
            )
            _reset_cache()
            lazy_action_cache.set(key, result, expire=expire)
            return result


def lazy_action(expire=None, cache=None):
    global lazy_action_cache
    lazy_action_cache = cache if cache else lazy_action_cache

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return _get_or_run_and_set(
                (inspect.getabsfile(func), func.__name__, args, tuple(kwargs.items())),
                func,
                args,
                kwargs,
                expire,
            )

        return wrapper

    return decorator
