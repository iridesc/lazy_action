import os
import time

from lazy_action.lazy_action import lazy_action, Cache


customer_cache = Cache(".cache")


@lazy_action(expire=5, cache=customer_cache)
def test_customer_cache(t):
    print(f"wait for {t}s")
    time.sleep(t)
    return time.time()


@lazy_action(expire=15)
def test(t):
    print(f"wait for {t}s")
    time.sleep(t)
    return time.time()


def destroy_cache():
    lazy_action_folder = ".lazy_action_cache"
    for name in os.listdir(lazy_action_folder):
        if name.startswith("cache-"):
            print(f"->  destroy cache {name}")
            with open(f".lazy_action_cache/{name}/cache.db", "w") as f:
                f.write("-------------")
        
def test_cache_recovery():

    print("start")
    print(test(10))
    # destroy_cache()
    print("start again")
    print(test(10))


if __name__ == "__main__":
    destroy_cache()
    test(1)
    test(2)
    test(1)
    test(1)
    test(2)
    test(1)

    # test_customer_cache(1)
    # test_customer_cache(2)
    # test_customer_cache(1)

    # test_cache_recovery()