import asyncio
import threading
import functools
import inspect
from typing import Callable, Any, TypeVar, cast, Optional, ParamSpec

P = ParamSpec('P')
T = TypeVar('T')

class AsyncToSync:
    """
    非同期メソッドを同期的に実行するためのラッパークラス
    パフォーマンスを優先し、スレッドプールを使用
    """
    
    _loop_thread: Optional[threading.Thread] = None
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _running: bool = False
    _lock: threading.Lock = threading.Lock()
    
    @classmethod
    def ensure_loop(cls) -> asyncio.AbstractEventLoop:
        """イベントループが実行中であることを確認する"""
        with cls._lock:
            if cls._loop is None or not cls._running:
                if cls._loop_thread is not None:
                    try:
                        cls._loop.call_soon_threadsafe(cls._loop.stop)
                        cls._loop_thread.join(timeout=1.0)
                    except Exception:
                        pass
                
                # 新しいイベントループを作成
                loop = asyncio.new_event_loop()
                
                def run_loop():
                    asyncio.set_event_loop(loop)
                    loop.run_forever()
                    
                thread = threading.Thread(target=run_loop, daemon=True)
                thread.start()
                
                cls._loop = loop
                cls._loop_thread = thread
                cls._running = True
                
            return cls._loop
    
    @classmethod
    def stop_loop(cls):
        """イベントループを停止する"""
        with cls._lock:
            if cls._loop is not None and cls._running:
                cls._loop.call_soon_threadsafe(cls._loop.stop)
                if cls._loop_thread is not None:
                    cls._loop_thread.join(timeout=1.0)
                cls._running = False
    
    @classmethod
    def wrap_method(cls, method: Callable[P, T]) -> Callable[P, T]:
        """
        クラスメソッドをラップして非同期関数を同期的に実行する
        
        Args:
            method: ラップする非同期メソッド
            
        Returns:
            同期的に動作するラップされたメソッド
        """
        @functools.wraps(method)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            if not inspect.iscoroutinefunction(method):
                return method(*args, **kwargs)
            
            loop = cls.ensure_loop()
            future = asyncio.run_coroutine_threadsafe(method(*args, **kwargs), loop)
            
            try:
                # タイムアウトなしで結果を待機
                return future.result()
            except Exception as e:
                # エラーを伝播させる
                raise e
        
        return cast(Callable[P, T], wrapper)
    
    @classmethod
    def wrap_class(cls, target_class: type) -> type:
        """
        クラスの全ての非同期メソッドをラップする
        
        Args:
            target_class: ラップするクラス
            
        Returns:
            同じクラスだが非同期メソッドがラップされたもの
        """
        for name, method in inspect.getmembers(target_class, predicate=inspect.isfunction):
            if inspect.iscoroutinefunction(method):
                setattr(target_class, name, cls.wrap_method(method))
        
        return target_class

# 使用例
if __name__ == "__main__":
    # サンプルの非同期クラス
    class AsyncExample:
        async def async_method(self, x: int, y: int) -> int:
            await asyncio.sleep(1)  # 非同期の処理をシミュレート
            return x + y
        
        def sync_method(self, x: int, y: int) -> int:
            return x * y
    
    # クラス全体をラップする方法
    SyncExample = AsyncToSync.wrap_class(AsyncExample)
    obj = SyncExample()
    
    # 同期的に呼び出せるようになる
    result = obj.async_method(5, 3)
    print(f"Result: {result}")  # 非同期メソッドの結果: 8
    
    # または個別のメソッドだけをラップする方法
    async_obj = AsyncExample()
    async_obj.async_method = AsyncToSync.wrap_method(async_obj.async_method)
    
    # こちらも同期的に呼び出せる
    result2 = async_obj.async_method(10, 20)
    print(f"Result 2: {result2}")  # 30
    
    # プログラム終了時にループを停止する
    AsyncToSync.stop_loop()
