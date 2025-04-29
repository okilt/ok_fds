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
