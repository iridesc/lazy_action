好的，恭喜您成功发布到 PyPI！

我已经根据您提供的信息更新了文档，主要修改了安装和导入部分，使其更符合一个标准库的用法。

-----

## `lazy_action` 文档

### 概述 📝

`lazy_action` 是一个功能强大且易于使用的 Python **函数缓存装饰器**。它的核心目标是：将耗时函数的执行结果保存到磁盘上，当下次使用完全相同的参数调用该函数时，直接从磁盘缓存中读取结果，从而避免重复执行，极大地提升程序运行效率。

它底层使用了 `diskcache` 库，提供了持久化、自动管理的缓存能力。

### 核心功能 ✨

  * **装饰器驱动**: 只需在你的函数上添加一行 `@lazy_action()` 即可启用缓存。
  * **持久化缓存**: 缓存结果保存在本地磁盘的 `.lazy_action_cache` 文件夹中，即使程序重新启动，缓存依然有效。
  * **智能缓存键**: 自动根据**函数所在的模块、函数名、位置参数和关键字参数**生成唯一的缓存键，确保只有在输入完全相同时才命中缓存。
  * **自动缓存管理**: 能够自动创建、加载和清理缓存文件。
  * **强大的容错性**: 当检测到缓存文件损坏 (`DatabaseError`) 或其他读写异常时，会自动重置并创建新的缓存，保证程序的健壮性。
  * **支持缓存过期**: 可以为缓存设置一个过期时间（TTL），过期后缓存将自动失效。

-----

### 安装 📦

该库已发布到 PyPI，您可以使用 pip 轻松安装：

```bash
pip install lazy_action
```

安装过程会自动处理并安装其依赖库 (`diskcache`)。

-----

### 如何使用 🚀

**基础示例：**

首先从库中导入 `lazy_action` 装饰器，并将其应用到你想要缓存结果的函数上。

```python
import time
from lazy_action import lazy_action

# 使用装饰器，对 slow_function 的结果进行缓存
@lazy_action()
def slow_function(name):
    print(f"函数正在执行... 为 '{name}' 进行复杂计算...")
    time.sleep(3)  # 模拟一个耗时3秒的操作
    result = f"Hello, {name}!"
    print("计算完成!")
    return result

# --- 第一次调用 ---
start_time = time.time()
print(f"第一次调用的结果: {slow_function('World')}")
print(f"耗时: {time.time() - start_time:.2f} 秒\n")

# --- 第二次调用 (参数相同) ---
start_time = time.time()
print(f"第二次调用的结果: {slow_function('World')}") # 将直接从缓存读取
print(f"耗时: {time.time() - start_time:.2f} 秒\n")
```

**输出结果：**

```
lazy_action >> create cache folder
lazy_action >> cache reset to .lazy_action_cache/cache-1721554000.12345
函数正在执行... 为 'World' 进行复杂计算...
计算完成!
第一次调用的结果: Hello, World!
耗时: 3.01 秒

第二次调用的结果: Hello, World!
耗时: 0.00 秒
```

**带过期时间的示例：**

你可以给 `lazy_action` 传递一个 `expire` 参数（单位为秒），让缓存在指定时间后失效。

```python
@lazy_action(expire=5) # 设置缓存5秒后过期
def get_data_from_remote():
    print("正在从远程API获取数据...")
    time.sleep(2)
    return {"data": "some important data"}

get_data_from_remote() # 第一次执行，耗时2秒
get_data_from_remote() # 5秒内再次调用，直接返回缓存，耗时0秒
time.sleep(6)
get_data_from_remote() # 超过5秒，缓存失效，重新执行，耗时2秒
```

-----

### 工作原理 ⚙️

1.  **缓存存储**:

      * 首次运行时，`lazy_action` 会在当前工作目录下创建一个名为 `.lazy_action_cache` 的隐藏文件夹。
      * 在这个文件夹内部，它会创建一个以时间戳命名的子文件夹（如 `cache-1721554000.12345`），用于存放实际的缓存数据（由 `diskcache` 管理）。
      * 程序再次启动时，会自动加载最新的那个缓存文件夹。

2.  **缓存键生成**:

      * 为了确保缓存的唯一性和准确性，`lazy_action` 使用了一个复合键：`(函数绝对路径, 函数名, 位置参数元组, 关键字参数元组)`。
      * 这意味着，即使两个不同文件中有同名函数，它们的缓存也是隔离的。参数的任何变化（包括顺序和值）都会导致缓存未命中。

3.  **核心逻辑 (`_get_or_run_and_set`)**:

      * 当装饰的函数被调用时，它首先用上述规则生成缓存键。
      * 检查该键是否存在于缓存中。
      * **如果存在**：直接返回缓存中的值。
      * **如果不存在**：执行原始函数，获取结果，将结果存入缓存，然后返回结果。

4.  **错误处理**:

      * 如果在任何读写缓存的环节（检查、获取、设置）发生异常，特别是 `sqlite3.DatabaseError`（表明底层数据库文件损坏），`_reset_cache` 函数会被调用。
      * `_reset_cache` 会清理掉所有旧的缓存实例，并创建一个全新的、干净的缓存目录，从而保证了即使在缓存损坏的情况下，程序也能继续正常运行（只是会暂时失去缓存效果）。