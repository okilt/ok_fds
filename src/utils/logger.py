# logging_utils.py (または関連モジュール)
import logging
import contextvars

# プログレスバーがアクティブかどうかを示すコンテキスト変数
# default=False で、通常は非アクティブ状態
progress_active_context: contextvars.ContextVar[bool] = contextvars.ContextVar(
    'progress_active', default=False
)

class ProgressFilter(logging.Filter):
    """
    プログレスバー表示中に指定レベル以下のログを抑制するフィルター。
    progress_active_context の値に基づいてフィルタリングを行う。
    """
    def __init__(self, suppress_level=logging.INFO, name=''):
        super().__init__(name)
        self.suppress_level = suppress_level
        # デバッグ用: フィルターが初期化されたことをログに出力
        # logging.getLogger(__name__).debug(f"ProgressFilter initialized for level <= {logging.getLevelName(suppress_level)}")

    def filter(self, record: logging.LogRecord) -> bool:
        """
        ログレコードをフィルタリングする。
        Trueを返すとログは処理され、Falseを返すと破棄される。
        """
        # コンテキスト変数を取得 (現在のコンテキストでの値)
        is_progress_active = progress_active_context.get()

        # デバッグ用: フィルタリングの判断基準を出力
        # logger = logging.getLogger(self.name) # フィルター自身のロガーを使う場合
        # logger.debug(f"Filtering record (Level: {record.levelname}, ProgressActive: {is_progress_active})")

        if is_progress_active and record.levelno <= self.suppress_level:
            # プログレス表示中で、抑制レベル以下の場合 -> ログを破棄 (False)
            # logger.debug(f"  -> Record suppressed.")
            return False
        else:
            # 上記以外の場合 -> ログを処理 (True)
            # logger.debug(f"  -> Record allowed.")
            return True


# main_app.py や初期設定を行う場所
import logging
from rich.logging import RichHandler
from rich.console import Console
# カスタムフィルターとコンテキスト変数をインポート
from logging_utils import ProgressFilter, progress_active_context

log = logging.getLogger("my_app") # アプリケーション全体のロガーなど
log.setLevel(logging.DEBUG) # ロガー自体は広く受け付ける

# RichHandler の設定 (stderr推奨)
log_console = Console(stderr=True)
console_handler = RichHandler(
    level=logging.DEBUG, # ★ ハンドラレベルは DEBUG のまま or INFO でも可 ★
                         #    実際のフィルタリングは Filter が行う
    console=log_console,
    show_path=False,
    markup=True
)

# ★★★ フィルターをハンドラに追加 ★★★
# suppress_level=logging.INFO でINFO以下を抑制対象にする
console_handler.addFilter(ProgressFilter(suppress_level=logging.INFO))
# ---------------------------------

log.addHandler(console_handler)

# (他のハンドラ設定...)

# --- ここまで初期設定 ---

# 以降のコードで logger = logging.getLogger(__name__) を使ってログ出力

# プログレス表示を行う関数 (例: main_with_context_wrapper)
import asyncio
from progress_manager import AsyncProgressContext # 前の回答のコンテキストマネージャ
# from your_progress_utils import run_coroutines_with_progress_indexed # ラッパー関数
# from logging_utils import progress_active_context # コンテキスト変数

async def main_with_context_filter():
    # ... (Progressカラム定義、コルーチン準備など) ...
    progress_columns = [...]
    coroutines_group_a = [...]
    coroutines_group_b = [...]

    log.info("--- Starting process with context filter ---")
    log.info("This INFO message appears BEFORE progress starts.")

    # ★ コンテキスト変数を設定してプログレスブロックを実行 ★
    token = progress_active_context.set(True) # プログレス開始前に True に設定
    log.debug("progress_active_context set to True") # DEBUGログ (表示されるかはハンドラレベル次第)

    try:
        async with AsyncProgressContext(*progress_columns, transient=False) as progress:
            log.warning("Inside async context (Warning log - should appear)") # WARNINGは表示される
            log.info("Inside async context (Info log - should be FILTERED OUT by ProgressFilter)") # INFOは抑制される

            # --- ラッパーを使ってタスクを実行 ---
            log.debug("Launching concurrent group processing (Debug log - filtered out if handler level > DEBUG)")
            wrapper_results = await asyncio.gather(
                run_coroutines_with_progress_indexed( # ラッパー内のINFO/DEBUGもフィルタリングされる
                    coroutines_group_a, "Processing Group A", progress, progress.add_task("Group A", ...), ...
                ),
                run_coroutines_with_progress_indexed(
                    coroutines_group_b, "Processing Group B", progress, progress.add_task("Group B", ...), ...
                ),
                return_exceptions=True
            )
            log.warning("Concurrent group processing finished (Warning log).") # WARNINGは表示される

    except Exception as e:
         log.exception("An error occurred during processing:")
    finally:
        # ★ 必ずコンテキスト変数を元に戻す ★
        progress_active_context.reset(token)
        log.debug("progress_active_context reset to previous value.")

    log.info("--- Process finished ---")
    log.info("This INFO message appears AFTER progress context is reset.") # これは表示される

# --- 実行 ---
# if __name__ == "__main__":
#     # ... (必要なインポートとダミー実装) ...
#     asyncio.run(main_with_context_filter())
