import time

from lazy_action.lazy_action import lazy_action, Cache


customer_cache = Cache(".cache")


@lazy_action(expire=5, cache=customer_cache)
def test_customer_cache(t):
    print(f"wait for {t}s")
    time.sleep(t)
    return time.time()


@lazy_action(expire=5)
def test(t):
    print(f"wait for {t}s")
    time.sleep(t)
    return time.time()


if __name__ == "__main__":
    test(1)
    test(2)
    test(1)

    test_customer_cache(1)
    test_customer_cache(2)
    test_customer_cache(1)
