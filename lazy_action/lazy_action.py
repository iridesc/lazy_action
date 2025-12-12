import inspect
import threading
import time
import traceback
from diskcache import Cache
import functools
import shutil
from sqlite3 import DatabaseError
import os
import portalocker
from cachelib import SimpleCache
import logging

logging.basicConfig(
    level=logging.WARN,
    format="%(asctime)s | %(levelname)s: lazy_action: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MEMORY_CACHE_THRESHOLD = 10000

LAZY_ACTION_FILE_PATH = os.path.abspath(os.getenv("LAZY_ACTION_FILE_PATH", "./")) 
if not os.path.exists(LAZY_ACTION_FILE_PATH):
    raise Exception(f"env LAZY_ACTION_FILE_PATH={LAZY_ACTION_FILE_PATH} does not exist!")

# 缓存目录
lazy_action_folder = os.path.join(LAZY_ACTION_FILE_PATH, ".lazy_action") 

# 执行reset 时的锁
disk_cache_reset_lock_path = os.path.join(
    LAZY_ACTION_FILE_PATH, ".disk_cache_reset.lock"
) 

# 当前缓存实例使用的路径
disk_cache_path = ""
disk_cache = None


memory_cache_action_lock = threading.Lock()
memory_cache = None



def _del_path(path):
    prefix = f"_del_path path={path}"
    if not os.path.exists(path):
        return

    if os.path.isdir(path):
        try:
            shutil.rmtree(path)

        except Exception as e:
            logger.error(f"{prefix} delete folder error:  {e}")
            for file_inside in os.listdir(path):
                _del_path(os.path.join(path, file_inside))

    else:
        try:
            os.remove(path)
        except Exception as e:
            logger.error(f"{prefix} delete file error: {e}")


def _rm_disk_caches():
    for name in os.listdir(lazy_action_folder):
        if name.startswith("disk_cache_"):
            target_folder = os.path.join(lazy_action_folder, name)
            _del_path(target_folder)


def _check_and_init_lazy_action_folder():
    if not os.path.exists(lazy_action_folder):
        os.makedirs(lazy_action_folder, exist_ok=True)
    if not os.path.isdir(lazy_action_folder):
        os.remove(lazy_action_folder)
        os.makedirs(lazy_action_folder, exist_ok=True)


def _reset_disk_cache():
    global disk_cache_path
    global disk_cache

    with portalocker.Lock(
        disk_cache_reset_lock_path, mode="a", timeout=None, flags=portalocker.LOCK_EX
    ):
        if disk_cache is not None:
            try:
                disk_cache.close()  # 尝试关闭底层连接
            except Exception as close_e:
                logger.error(
                    f"_reset_disk_cache: Error closing cache explicitly: {close_e}"
                )
        disk_cache = None

        _check_and_init_lazy_action_folder()

        _rm_disk_caches()
        disk_cache_path = os.path.join(lazy_action_folder, f"disk_cache_{time.time()}")
        disk_cache = Cache(disk_cache_path)
        logger.error(f"_reset_disk_cache: cache reset to {disk_cache_path}")


def _init_disk_cache():
    global disk_cache, disk_cache_path
    if disk_cache and disk_cache_path:
        return
    try:
        _check_and_init_lazy_action_folder()
        names = os.listdir(lazy_action_folder)
        names.sort(reverse=True)

        for name in names:
            if name.startswith("disk_cache_"):
                disk_cache_path = os.path.join(lazy_action_folder, name)
                disk_cache = Cache(disk_cache_path)
                break

        else:
            _reset_disk_cache()

    except DatabaseError:
        logger.error("_init_disk_cache: DatabaseError remove cache file")
        _reset_disk_cache()

    except Exception as e:
        logger.error(f"_init_disk_cache: unknown error, reset cache! e={e}")
        _reset_disk_cache()


def _get_from_disk(key):
    global disk_cache

    try:
        return disk_cache.get(key)
    except Exception as e:
        logger.error(
            f"_get_from_disk: unknown error in lazy_action fetch result, reset cache! e={e}"
        )
        _reset_disk_cache()
        return None


def _set_in_disk(key, t_result, rest_time):
    global disk_cache
    try:
        disk_cache.set(key, t_result, expire=rest_time)
    except Exception as e:
        logger.error(
            f"_set_in_disk unknown error in lazy_action set result reset cache! e={e}"
        )
        _reset_disk_cache()
        try:
            disk_cache.set(key, t_result, expire=rest_time)
        except Exception as e:
            logger.error(
                f"_set_in_disk unknown error again in lazy_action when set result in new cache! e={traceback.format_exc()}"
            )


def _init_memory_cache(reset=False):
    global memory_cache
    if memory_cache is None or reset:
        memory_cache = SimpleCache(threshold=MEMORY_CACHE_THRESHOLD)
    


def _get_from_memory(key):
    global memory_cache
    try:
        with memory_cache_action_lock:
            return memory_cache.get(key) 
    except Exception as e:
        logger.error(f"_get_from_memory: global memory cache get error: {e}")
        _init_memory_cache(
            reset=True,
        )
        return None


def _set_in_memory(key, t_result, rest_time):
    try:
        with memory_cache_action_lock:
            memory_cache.set(key, t_result, timeout=rest_time)
    except Exception as e:
        logger.error(f"_set_in_memory: global memory cache set error: {e}")

        _init_memory_cache(
            reset=True,
        )


def _get_or_run_and_set(
    key,
    func,
    args,
    kwargs,
    expire,
    getter_and_setters,
):
    global memory_cache
    global disk_cache

    now_time = time.time()
    promotion_setter = list()
    for getter, setter in getter_and_setters:
        t_result = getter(key)

        if t_result is None:
            promotion_setter.append(setter)
            continue

        for setter in promotion_setter:
            if expire is None:
                setter(key, t_result, None)
                continue
            rest_time = (t_result[1] + expire) - now_time 
            if rest_time > 0:
                setter(key, t_result, rest_time)

        return t_result

    t_result = (
        func(
            *args,
            **kwargs,
        ), 
        time.time()
    )
    for _, setter in getter_and_setters:
        setter(key, t_result, expire)

    return t_result


def lazy_action(expire=60, mode="disk"):
    """
    多级缓存装饰器，用于缓存函数的执行结果。
    A multi-level caching decorator for caching function execution results.

    该装饰器支持内存 (L1) 和磁盘 (L2) 两级缓存，并内置了高级并发安全和
    故障容错机制，确保缓存系统的可靠性和高性能。它能精确同步各层缓存的 TTL。
    It supports in-memory (L1) and disk (L2) caching with built-in advanced
    concurrency safety and fault tolerance, ensuring system reliability and high performance.
    It precisely synchronizes the Time-To-Live (TTL) across cache layers, including L2-to-L1 promotion.

    :param expire: 缓存的存活时间 (Time To Live, TTL)，单位为秒 (s)。
                   此时间独立应用于每个缓存项。如果设置为 None，则缓存项永不过期。
                   Cache Time To Live (TTL) in seconds (s). This time is applied
                   independently to each key. If set to None, the cache entry will not expire.
    :type expire: int 或 float, optional (int or float, optional)
    :param mode: 缓存模式。The caching mode to use.
                - "mix": 启用 L1 (内存) -> L2 (磁盘) 的查找级联，
                  以及 L2 命中后数据自动 **提升 (Promotion)** 回 L1 的机制。
                  Enables cascading lookups (L1 -> L2) and automatic **promotion**
                  of L2 hits back to L1 memory.
                - "memory": 仅使用 L1 内存缓存 (SimpleCache)。
                  Uses only the L1 in-memory cache (SimpleCache).
                - "disk"(默认/default): 仅使用 L2 磁盘缓存 (diskcache)，支持多进程访问和持久化。
                  Uses only the L2 disk cache (diskcache), supporting multi-process
                  access and persistence.
    :type mode: str, optional
    :raises Exception: 如果 mode 参数不是 'mix', 'memory', 或 'disk' 之一。
                       If the mode parameter is not one of 'mix', 'memory', or 'disk'.
    :return: 缓存装饰器函数。The caching decorator function.
    """

    def decorator(func):
        fixed_key_prefix = (
            inspect.getabsfile(func),  # 昂贵操作，只执行一次！
            func.__name__,
        )
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # i_start = time.time()
            key = (
                fixed_key_prefix,
                args,
                tuple(kwargs.items()),
            )
            # global init_cost
            # init_cost += time.time() - i_start
            # print(f"init cost: {init_cost:.2f}s")
            if mode == "disk":
                _init_disk_cache()
                return _get_or_run_and_set(
                    key,
                    func,
                    args,
                    kwargs,
                    expire,
                    getter_and_setters=[
                        (
                            _get_from_disk,
                            _set_in_disk,
                        ),
                    ],
                )[0]
            elif mode == "memory":
                _init_memory_cache()
                return _get_or_run_and_set(
                    key,
                    func,
                    args,
                    kwargs,
                    expire,
                    getter_and_setters=[
                        (
                            _get_from_memory,
                            _set_in_memory,
                        ),
                    ],
                )[0]

            elif mode == "mix":
                _init_disk_cache()
                _init_memory_cache()

                return _get_or_run_and_set(
                    key,
                    func,
                    args,
                    kwargs,
                    expire,
                    getter_and_setters=[
                        (
                            _get_from_memory,
                            _set_in_memory,
                        ),
                        (
                            _get_from_disk,
                            _set_in_disk,
                        ),
                    ],
                )[0]

            else:
                raise Exception("mode must be one of [mix, disk, memory]")

        return wrapper

    return decorator
