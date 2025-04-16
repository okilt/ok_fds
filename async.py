import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

# CPUバウンド処理の例
def cpu_bound_task(n):
    # 重い計算処理のシミュレーション
    result = 0
    for i in range(n):
        result += i * i
    return result

# I/Oバウンド処理の例
async def io_bound_task(delay, name):
    print(f"{name} 開始")
    await asyncio.sleep(delay)  # I/O待ちのシミュレーション
    print(f"{name} 完了")
    return f"{name}の結果"

async def main():
    # CPUバウンド処理をスレッドプールで実行
    result1 = await asyncio.to_thread(cpu_bound_task, 10_000_000)
    print(f"CPUバウンド結果: {result1}")
    
    # 複数のI/Oバウンド処理を並行実行
    tasks = [
        io_bound_task(1, "タスク1"),
        io_bound_task(2, "タスク2"),
        io_bound_task(1.5, "タスク3")
    ]
    
    # エラーハンドリングの例
    try:
        # 複数タスクの並行実行
        results = await asyncio.gather(*tasks, return_exceptions=True)
        print(f"I/Oバウンド結果: {results}")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    
    # セマフォによる並行数制限の例
    semaphore = asyncio.Semaphore(2)  # 最大2つのタスクを同時実行
    
    async def limited_task(name):
        async with semaphore:
            print(f"{name} 実行中")
            await asyncio.sleep(1)
            return f"{name} 完了"
    
    # 5つのタスクを作成するが、セマフォにより同時実行は2つまで
    limited_tasks = [limited_task(f"制限付きタスク{i}") for i in range(5)]
    limited_results = await asyncio.gather(*limited_tasks)
    print(limited_results)

# メインエントリーポイント
if __name__ == "__main__":
    asyncio.run(main(), debug=True)  # デバッグモード有効化


## 1. シングルトンパターンによるイベントループ管理

# event_loop.py
import asyncio
import contextvars
import sys

# アプリケーション全体で単一のイベントループを管理するクラス
class EventLoopManager:
    _instance = None
    _loop = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EventLoopManager, cls).__new__(cls)
            # iPythonかどうかを検出
            cls._is_ipython = 'IPython' in sys.modules
        return cls._instance
    
    def get_loop(self):
        """現在のイベントループを取得するか、なければ新規作成"""
        if self._loop is None or self._loop.is_closed():
            try:
                # 既存のイベントループがあれば取得
                self._loop = asyncio.get_event_loop()
            except RuntimeError:
                # イベントループがない場合は新規作成（スレッドごとに必要）
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop
    
    def run_async(self, coro):
        """非同期関数を実行するためのヘルパーメソッド"""
        loop = self.get_loop()
        
        if self._is_ipython:
            # iPythonでは既にイベントループが実行されている可能性がある
            if loop.is_running():
                # nest_asyncioがインポートされていることを前提
                import nest_asyncio
                nest_asyncio.apply(loop)
                return loop.run_until_complete(coro)
            else:
                return loop.run_until_complete(coro)
        else:
            # 通常のPythonスクリプトの場合
            if not loop.is_running():
                return loop.run_until_complete(coro)
            else:
                # 既に実行中の場合は新しいタスクとして追加
                return asyncio.run_coroutine_threadsafe(coro, loop).result()

# 簡単にアクセスするためのグローバルインスタンス
loop_manager = EventLoopManager()

## 2. アプリケーション内での使用例

# module1.py
import asyncio
from event_loop import loop_manager

class Service1:
    async def process_data(self, data):
        await asyncio.sleep(1)  # 非同期I/O操作
        return f"処理済み: {data}"
    
    def run_process(self, data):
        # 非同期メソッドを同期的に呼び出すラッパー
        return loop_manager.run_async(self.process_data(data))

# module2.py
import asyncio
from event_loop import loop_manager

class Service2:
    async def fetch_data(self, source):
        await asyncio.sleep(0.5)  # 非同期I/O操作
        return f"{source}からのデータ"
    
    def get_data(self, source):
        # 非同期メソッドを同期的に呼び出すラッパー
        return loop_manager.run_async(self.fetch_data(source))

# main.py
import asyncio
from event_loop import loop_manager
from module1 import Service1
from module2 import Service2

async def main():
    service1 = Service1()
    service2 = Service2()
    
    # 非同期関数として呼び出す場合
    data = await service2.fetch_data("DB")
    result = await service1.process_data(data)
    print(result)
    
    # または同期的に呼び出す場合
    # data = service2.get_data("DB")
    # result = service1.run_process(data)
    # print(result)

if __name__ == "__main__":
    # メインスクリプトからの実行
    loop_manager.run_async(main())

## 3. iPythonでの使用に必要な追加設定

# iPythonで非同期コードを実行する場合は、`nest_asyncio`を使用して入れ子のイベントループを許可する必要があります。これをプロジェクトの初期化時に行います：

# ipython_setup.py
def setup_for_ipython():
    try:
        import nest_asyncio
        import asyncio
        from event_loop import loop_manager
        
        # 現在のイベントループに対してnest_asyncioを適用
        nest_asyncio.apply(loop_manager.get_loop())
        print("iPython環境用の非同期設定が完了しました")
    except ImportError:
        print("iPython用の設定には nest_asyncio パッケージが必要です")
        print("pip install nest_asyncio でインストールしてください")

# iPythonを使用するユーザーは、セッションの最初に以下を実行するよう指示します：

from ipython_setup import setup_for_ipython
setup_for_ipython()

## 4. 依存関係の注入パターン

# 大規模なアプリケーションでは、イベントループを依存関係として注入することで管理しやすくなります：


# dependency.py
from event_loop import loop_manager

class AsyncContext:
    def __init__(self, loop=None):
        self.loop = loop or loop_manager.get_loop()
        
    async def run_task(self, coro):
        return await coro
        
    def run_sync(self, coro):
        return loop_manager.run_async(coro)

# アプリケーション全体で使用する非同期コンテキスト
async_context = AsyncContext()


# これにより、各クラスは直接イベントループを操作する代わりに、この共通コンテキストを使用できます。

## 5. テスト用の考慮事項

# テスト時には専用のイベントループを作成し、テスト間で分離することが重要です：


# test_async.py
import asyncio
import pytest

@pytest.fixture
def event_loop():
    """テストごとに新しいイベントループを作成"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    
async def test_async_function():
    # テスト用の非同期コード
    result = await some_async_function()
    assert result == expected_value

# このアプローチを使用すると、複数のファイルやクラスにまたがる場合でも、単一のイベントループを管理し、iPythonユーザーにも対応できます。また、テスト環境でも適切に動作します。​​​​​​​​​​​​​​​​
