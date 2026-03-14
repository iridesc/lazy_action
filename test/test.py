import logging
import os
import random
import time
import shutil
from venv import logger
from importlib.metadata import version as get_version
from typing import List

from lazy_action.lazy_action import (
    lazy_action,
    lazy_action_folder,
    _init_memory_cache,
    _init_redis_cache,
)


os.environ["LAZY_ACTION_REDIS_URL"] = "redis://:passwd@localhost:6379"


# ----------------------------------------------------------------------
# 辅助函数
# ----------------------------------------------------------------------


def cleanup_cache_dirs():
    """清理所有缓存目录，确保测试环境干净。"""
    if os.path.exists(lazy_action_folder):
        print(f"\n--- 清理旧缓存目录: {lazy_action_folder} ---")
        shutil.rmtree(lazy_action_folder)

    # 重新初始化确保文件夹存在
    if not os.path.exists(lazy_action_folder):
        os.makedirs(lazy_action_folder, exist_ok=True)


def simulate_disk_corruption():
    """模拟 diskcache 文件被破坏，强制触发 L2 异常和重置。"""
    try:
        # 找到最新的 disk_cache 路径
        names = os.listdir(lazy_action_folder)
        disk_dirs = [
            os.path.join(lazy_action_folder, n)
            for n in names
            if n.startswith("disk_cache_")
        ]
        if disk_dirs:
            latest_cache_dir = max(disk_dirs, key=os.path.getctime)
            db_path = os.path.join(latest_cache_dir, "cache.db")

            # 用无效数据覆盖数据库文件
            with open(db_path, "wb") as f:
                f.write(b"CORRUPTED DATA")
            logger.info(f"simulate_disk_corruption ✅ 成功破坏 L2 缓存文件: {db_path}")
        else:
            logger.warning("simulate_disk_corruption ❌ 未找到 L2 缓存文件进行破坏。")
    except Exception as e:
        logger.warning(f"simulate_disk_corruption ❌ 模拟破坏失败: {e}")


def generate_production_like_key_sequence_pure_python(
    total_unique_keys: int,  # 缓存中唯一的 Key 总数 (例如：10000)
    sequence_length: int,  # 需要生成的 Key 序列长度 (例如：100000)
    hot_key_ratio: float = 0.05,  # 热点 Key 占总 Key 的比例 (例如：5%)
    hot_access_ratio: float = 0.8,  # 热点 Key 占总访问量的比例 (例如：80%)
) -> List[str]:
    """
    生成一个遵循二八定律（或类似）的缓存 Key 访问序列，仅使用 Python 标准库。

    参数:
    - total_unique_keys (int): 唯一的 Key 总数。
    - sequence_length (int): 生成的访问序列长度。
    - hot_key_ratio (float): 热点 Key 占 unique_keys 的比例 (0.0 到 1.0)。
    - hot_access_ratio (float): 热点 Key 占总访问 sequence_length 的比例 (0.0 到 1.0)。

    返回:
    - List[str]: 包含 Key 的访问序列列表。
    """

    if not (0.0 < hot_key_ratio < 1.0 and 0.0 < hot_access_ratio < 1.0):
        raise ValueError("hot_key_ratio 和 hot_access_ratio 必须在 (0.0, 1.0) 之间。")

    # --- 1. 定义 Key 集合 ---

    # 1.1 计算热点 Key 和冷门 Key 的数量
    num_hot_keys = max(1, int(total_unique_keys * hot_key_ratio))

    # 生成 Key 列表（使用 'key_00001' 这种格式来模拟 ID）
    all_keys = [f"key_{i:05d}" for i in range(total_unique_keys)]

    # 分割热点 Key 和冷门 Key
    hot_keys = all_keys[:num_hot_keys]
    cold_keys = all_keys[num_hot_keys:]
    num_cold_keys = len(cold_keys)

    # --- 2. 定义访问次数 ---

    # 2.1 计算热点 Key 和冷门 Key 的总访问次数
    hot_access_count = int(sequence_length * hot_access_ratio)
    cold_access_count = sequence_length - hot_access_count

    # --- 3. 生成访问权重 (纯 Python 实现) ---

    # 3.1 热点 Key 的访问权重：模拟集中性（例如，线性递减权重）
    # 我们可以让第一个 Key 拥有最高的权重，然后线性递减。
    hot_weights: List[float] = []
    # 权重从 num_hot_keys 递减到 1
    for i in range(num_hot_keys, 0, -1):
        hot_weights.append(float(i))

    # 3.2 冷门 Key 的访问权重：模拟相对均匀，但仍有随机性
    # 给每个冷门 Key 一个接近但略有不同的随机权重
    cold_weights: List[float] = [random.uniform(0.5, 1.5) for _ in range(num_cold_keys)]

    # --- 4. 生成 Key 访问序列 ---

    sequence = []

    # 4.1 生成热点 Key 访问序列
    # random.choices 使用相对权重
    hot_sequence = random.choices(
        hot_keys,
        weights=hot_weights,
        k=hot_access_count,  # 抽取 N_hot 次
    )
    sequence.extend(hot_sequence)

    # 4.2 生成冷门 Key 访问序列
    cold_sequence = random.choices(
        cold_keys,
        weights=cold_weights,
        k=cold_access_count,  # 抽取 N_cold 次
    )
    sequence.extend(cold_sequence)

    # --- 5. 打乱序列以模拟真实访问的随机性 ---
    random.shuffle(sequence)

    return sequence


# ----------------------------------------------------------------------
# 测试函数
# ----------------------------------------------------------------------


@lazy_action(expire=2, mode="mix")
def test_mix_ttl_short(t):
    """短 TTL 混合模式测试，用于验证 L1/L2 写入和 L1 失效。"""
    print(f"\n[Func: test_mix_ttl_short] Wait {t}s, Args: {t}")
    time.sleep(t)
    return time.time()


@lazy_action(expire=10, mode="mix")
def test_mix_ttl_long(t):
    """长 TTL 混合模式测试，用于验证 L2 保持缓存。"""
    print(f"\n[Func: test_mix_ttl_long] Wait {t}s, Args: {t}")
    time.sleep(t)
    # 返回 None 以测试哨兵值 _MISSING 的处理
    return None


# ----------------------------------------------------------------------
# 测试用例
# ----------------------------------------------------------------------


def show_test_info(desc):
    print("=" * 50)
    print(f"{desc}")
    print("=" * 50)


log_prefix = "TEST:"


def test_case_basic_functionality():
    show_test_info("基本功能测试模式 的命中 与 失效测试")

    for mode in [
        "disk",
        "memory",
        "mix",
        "redis",
        "memory_redis"
    ]:

        @lazy_action(expire=3, mode=mode)
        def _test_case_basic_functionality(key):
            print(f"调用函数{key=}")
            time.sleep(1)
            return time.time()

        logger.info(f"{log_prefix} {mode=} ")
        logger.info(f"{log_prefix} 首次调用")

        r1 = _test_case_basic_functionality(mode)
        logger.info(f"{log_prefix} {r1=} ")

        r2 = _test_case_basic_functionality(mode)
        logger.info(f"{log_prefix} 立即第二次调用 应该与第一次一致 {r2=}")
        assert r1 == r2

        time.sleep(3.1)
        r3 = _test_case_basic_functionality(mode)
        logger.info(f"{log_prefix} 等待3秒后 第三次调用 缓存失效 r与之前不一致 {r3=}")
        assert r2 != r3
        logger.info(f"{log_prefix} {mode=} ✅测试通过")


def test_case_disk_corruption_recovery():
    show_test_info("磁盘容错与重置测试 mix 和 disk 模式")

    for mode in ["disk", "mix"]:
        print()

        @lazy_action(expire=3, mode=mode)
        def t():
            time.sleep(1)
            return time.time()

        logger.info(f"{log_prefix} {mode=} ")
        logger.info(f"{log_prefix} 首次调用")
        r1 = t()
        logger.info(f"{log_prefix} {r1=} ")

        logger.info(
            f"{log_prefix} 模拟 L2 磁盘缓存损坏 并 等待缓存超时 这样 下次两种模式肯定会执行disk缓存查询 从而触发磁盘异常缓存重置"
        )
        simulate_disk_corruption()
        time.sleep(3)

        logger.info(f"{log_prefix} 执行第二次查询")
        r2 = t()
        logger.info(
            f"{log_prefix} 模拟 L2 磁盘缓存损坏后的查询 会执行函数 就结果应该不一致 {r2=}"
        )
        assert r1 != r2


def test_case_memory_failure_recovery():
    show_test_info("内存缓存容错与重置测试 memory 和 mix 模式")

    for mode in ["memory", "mix"]:
        print()

        @lazy_action(expire=3, mode=mode)
        def t():
            time.sleep(1)
            return time.time()

        logger.info(f"{log_prefix} {mode=} ")
        logger.info(f"{log_prefix} 首次调用")
        r1 = t()
        logger.info(f"{log_prefix} {r1=} ")

        logger.info(
            f"{log_prefix} 模拟 内存缓存损坏 并 直接执行第二次查询 mix 会重建内存缓存 然后去 磁盘缓存查询 并进行缓存提升, memory 会直接重建缓存 并重新执行"
        )
        _init_memory_cache(reset=True)

        logger.info(f"{log_prefix} 执行第二次查询")
        r2 = t()
        logger.info(
            f"{log_prefix} 模拟 L2 损坏后的查询 会执行函数 就结果应该不一致 {r2=}"
        )
        assert r1 != r2

        logger.info(f"{log_prefix} 执行第三次查询")
        r3 = t()
        logger.info(
            f"{log_prefix} 缓存恢复后会把r2 存储在memory cache 结果应该与r2一致 {r3=}"
        )
        assert r2 == r3


class TestR:
    def __init__(self, v):
        self.v = v


class TestI:
    def __init__(self, v):
        self.v = v


def test_case_performance():
    show_test_info(
        "性能测试：比较 Memory, Disk, Mix, Redis, Memory+Redis 模式的缓存命中速度"
    )

    k_amount = 2000
    expire = 3

    keys = generate_production_like_key_sequence_pure_python(
        k_amount,
        k_amount * 100,
    )
    base_keys = [TestI(k) for k in set(keys)]
    keys = [TestI(k) for k in keys]

    @lazy_action(expire=expire, mode="disk")
    def t_disk(k):
        return TestR(k)

    @lazy_action(expire=expire, mode="memory")
    def t_memory(k):
        return TestR(k)

    @lazy_action(expire=expire, mode="mix")
    def t_mix(k):
        return TestR(k)

    @lazy_action(expire=expire, mode="memory_redis")
    def t_memory_redis(k):
        return TestR(k)

    @lazy_action(expire=expire, mode="redis")
    def t_redis(k):
        return TestR(k)

    def t(k):
        return TestR(k)

    mode_to_func = {
        "disk": t_disk,
        "memory": t_memory,
        "mix": t_mix,
        "redis": t_redis,
        "memory_redis": t_memory_redis,
        "base": t,

    }


    # warm up
    for index, k in enumerate(base_keys):
        if index % 1000 == 0:
            logger.info(f"{log_prefix} 预热进度: {index / k_amount * 100:.2f}%")

        for mode, func in mode_to_func.items():
            if mode == "base":
                continue
            func(k)

    logger.info(f"{log_prefix} 预热完成")

    mode_to_time = {
        "disk": 0,
        "memory": 0,
        "mix": 0,
        "redis": 0,
        "memory_redis": 0,
        "base": 0,
    }

    for mode, func in mode_to_func.items():
        start = time.time()
        for index, key in enumerate(keys):
            func(key)
            if index % 5000 == 0:
                logger.info(
                    f"{log_prefix} {mode=} 进度: {index / len(keys) * 100:.2f}%"
                )

        mode_to_time[mode] += time.time() - start

    logger.info(f"{log_prefix} {mode_to_time=}")

    print("\n" + "=" * 50)
    print("性能测试结果汇总")
    print("=" * 50)
    for mode, t in sorted(mode_to_time.items(), key=lambda x: x[1]):
        print(f"  {mode:10s}: {t:.3f}s")
    print("=" * 50)


def test_case_redis_cache():
    show_test_info("Redis 缓存测试")

    @lazy_action(expire=3, mode="redis")
    def t_redis():
        time.sleep(0.5)
        return time.time()

    logger.info(f"{log_prefix} 首次调用 Redis 缓存")
    r1 = t_redis()
    logger.info(f"{log_prefix} r1={r1}")

    logger.info(f"{log_prefix} 第二次调用 (应该命中缓存)")
    r2 = t_redis()
    logger.info(f"{log_prefix} r2={r2}")
    assert r1 == r2, "Redis 缓存未命中"

    logger.info(f"{log_prefix} 等待 3 秒后再次调用 (缓存应该过期)")
    time.sleep(3)
    r3 = t_redis()
    logger.info(f"{log_prefix} r3={r3}")
    assert r2 != r3, "Redis 缓存未过期"
    print("✅ Redis 缓存测试通过!")


def test_case_memory_redis_basic_functionality():
    show_test_info("Memory + Redis 模式基本功能测试")

    @lazy_action(expire=3, mode="memory_redis")
    def t():
        time.sleep(0.5)
        return time.time()

    logger.info(f"{log_prefix} 首次调用 (应该执行函数并写入 Redis 和 Memory)")
    r1 = t()
    logger.info(f"{log_prefix} r1={r1}")

    logger.info(f"{log_prefix} 第二次调用 (应该命中 Memory 缓存)")
    r2 = t()
    logger.info(f"{log_prefix} r2={r2}")
    assert r1 == r2, "Memory 缓存未命中"

    time.sleep(3)
    logger.info(f"{log_prefix} 等待 3 秒后调用 (Memory 过期，Redis 过期，应该重新执行)")
    r3 = t()
    logger.info(f"{log_prefix} r3={r3}")
    assert r2 != r3, "缓存未过期"

    print("✅ Memory+Redis 缓存测试通过!")


# def test_case_redis_env_missing():
#     show_test_info("Redis 环境变量未设置测试")

#     import importlib
#     import sys
#     import os


#     try:
#         import lazy_action.lazy_action as la_module
#         importlib.reload(la_module)

#         @la_module.lazy_action(expire=60, mode="redis")
#         def test_func():
#             return "test"

#         try:
#             test_func()
#             print("❌ 应该抛出异常但没有")
#             assert False
#         except Exception as e:
#             if "LAZY_ACTION_REDIS_URL" in str(e):
#                 print(f"✅ 正确抛出异常: {e}")
#             else:
#                 print(f"❌ 异常信息不正确: {e}")
#                 assert False
#     finally:
#         if original_url:
#             os.environ["LAZY_ACTION_REDIS_URL"] = original_url
#         importlib.reload(la_module)


def test_case_cache_promotion():
    show_test_info("缓存提升测试 (L2 -> L1)")

    @lazy_action(expire=60, mode="memory_redis")
    def t_promotion():
        return time.time()

    logger.info(f"{log_prefix} 第一次调用 - 写入 Redis 和 Memory")
    r1 = t_promotion()

    logger.info(f"{log_prefix} 第二次调用 - 应该从 Memory 提升")
    r2 = t_promotion()
    assert r1 == r2, "Memory 提升失败"
    logger.info(f"{log_prefix} Memory 缓存命中: r1={r1}, r2={r2}")

    _init_memory_cache(reset=True)
    logger.info(f"{log_prefix} 清空 Memory")

    logger.info(f"{log_prefix} 清空 Redis")

    logger.info(f"{log_prefix} 第三次调用 - Redis 和 Memory 都无数据，重新执行")
    r3 = t_promotion()
    assert r3 != r2, "应该重新执行"
    logger.info(f"{log_prefix} 重新执行: r3={r3}")

    logger.info(f"{log_prefix} 第四次调用 - 应该命中 Memory")
    r4 = t_promotion()
    assert r3 == r4, "Memory 提升失败"
    logger.info(f"{log_prefix} Memory 缓存命中: r4={r4}")

    print("✅ 缓存提升测试通过!")


# ----------------------------------------------------------------------
# 主执行
# ----------------------------------------------------------------------

if __name__ == "__main__":
    __version__ = get_version("lazy_action")

    print(f"📦 Version: {__version__}")
    logging.basicConfig(
        # level=logging.DEBUG,
        level=logging.INFO,
        force=True,
        format="%(asctime)s | %(levelname)s: lazy_action: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cleanup_cache_dirs()

    test_case_basic_functionality()
    # test_case_redis_env_missing()
    # test_case_disk_corruption_recovery()
    # test_case_memory_failure_recovery()
    # test_case_redis_cache()
    # test_case_memory_redis_basic_functionality()
    # test_case_cache_promotion()
    test_case_performance()

    # cleanup_cache_dirs()
    print(f"📦 Version: {__version__}")

    # print("\n\n=== 🎉 所有测试用例已成功运行！===")
