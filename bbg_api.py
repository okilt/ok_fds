import blpapi
import time
import threading
import queue
import logging
import configparser
import os
from collections import defaultdict
from datetime import datetime
import json
import asyncio # asyncio連携のため

# --- 定数定義 ---
# blpapi.Name オブジェクト (事前に生成しておくと効率が良い)
SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SESSION_TERMINATED      = blpapi.Name("SessionTerminated")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
SLOW_CONSUMER_WARNING   = blpapi.Name("SlowConsumerWarning")
SLOW_CONSUMER_WARNING_CLEARED = blpapi.Name("SlowConsumerWarningCleared")
DATA_LOSS               = blpapi.Name("DataLoss")
REQUEST_FAILURE         = blpapi.Name("RequestFailure")
ADMIN                   = blpapi.Name("Admin")
TIMEOUT                 = blpapi.Name("Timeout")

# Event Types
PARTIAL_RESPONSE        = blpapi.Event.EventType.PARTIAL_RESPONSE
RESPONSE                = blpapi.Event.EventType.RESPONSE
TIMEOUT_EVENT           = blpapi.Event.EventType.TIMEOUT # イベントタイプとしてのタイムアウト
ADMIN_EVENT             = blpapi.Event.EventType.ADMIN
SESSION_STATUS          = blpapi.Event.EventType.SESSION_STATUS
SERVICE_STATUS          = blpapi.Event.EventType.SERVICE_STATUS
# 他にも多くのイベントタイプが存在

# Message Types (Adminイベントなどで使用)
PERMISSION_REQUEST      = blpapi.Name("PermissionRequest")
RESOLUTION_SUCCESS      = blpapi.Name("ResolutionSuccess")
RESOLUTION_FAILURE      = blpapi.Name("ResolutionFailure")
# 他多数

# Data Element Names
SECURITY_DATA           = blpapi.Name("securityData")
SECURITY_NAME           = blpapi.Name("security")
FIELD_DATA              = blpapi.Name("fieldData")
FIELD_EXCEPTIONS        = blpapi.Name("fieldExceptions")
FIELD_ID                = blpapi.Name("fieldId")
ERROR_INFO              = blpapi.Name("errorInfo")
MESSAGE                 = blpapi.Name("message")
CATEGORY                = blpapi.Name("category")
SUBCATEGORY             = blpapi.Name("subcategory")
CODE                    = blpapi.Name("code")
SOURCE                  = blpapi.Name("source")
SECURITY_ERROR          = blpapi.Name("securityError")
RESPONSE_ERROR          = blpapi.Name("responseError")
REASON                  = blpapi.Name("reason") # RequestFailure内

# Intraday Data Names
BAR_DATA                = blpapi.Name("barData")
BAR_TICK_DATA           = blpapi.Name("barTickData")
TICK_DATA               = blpapi.Name("tickData")
TIME                    = blpapi.Name("time")
OPEN                    = blpapi.Name("open")
HIGH                    = blpapi.Name("high")
LOW                     = blpapi.Name("low")
CLOSE                   = blpapi.Name("close")
VOLUME                  = blpapi.Name("volume")
NUM_EVENTS              = blpapi.Name("numEvents")
VALUE                   = blpapi.Name("value") # Tickデータ用
TYPE                    = blpapi.Name("type") # Tickデータ用

# --- エラーカテゴリ (リトライ判断用) ---
# これらは例であり、実際のカテゴリ/サブカテゴリに合わせて調整が必要
RETRYABLE_CATEGORIES = {"TIMEOUT", "NETWORK_ERROR", "SERVER_ERROR"} # 仮のカテゴリ名
NON_RETRYABLE_CATEGORIES = {"BAD_SECURITY", "BAD_FIELD", "AUTHORIZATION_FAILURE", "INVALID_REQUEST"} # 仮のカテゴリ名


# --- ロガー設定 ---
def setup_logger(name='blpapi_wrapper', level=logging.INFO, log_file=None):
    """ロガーを設定するヘルパー関数"""
    logger = logging.getLogger(name)
    # 既にハンドラが設定されている場合は追加しない (重複ログ防止)
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s')

    # コンソールハンドラ
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # ファイルハンドラ (指定された場合)
    if log_file:
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # blpapiライブラリ自体のログレベルを調整 (冗長な場合)
    blpapi_logger = logging.getLogger('blpapi')
    blpapi_logger.setLevel(logging.WARNING) # 必要に応じて変更

    return logger

# --- 設定管理 ---
def load_config(config_file='config.ini'):
    """設定ファイルから接続情報などを読み込む"""
    config = configparser.ConfigParser()
    # デフォルト値
    defaults = {
        'host': 'localhost',
        'port': '8194',
        'timeout': '30',
        'max_retries': '3',
        'retry_delay': '2',
        'log_level': 'INFO',
        'log_file': '' # 空ならファイル出力しない
    }
    # 環境変数によるオーバーライド (例)
    defaults['host'] = os.environ.get('BLPAPI_HOST', defaults['host'])
    defaults['port'] = os.environ.get('BLPAPI_PORT', defaults['port'])
    # 他も同様に

    config['DEFAULT'] = defaults # パーサーにデフォルトを設定

    if os.path.exists(config_file):
        try:
            config.read(config_file, encoding='utf-8')
            print(f"Loaded configuration from {config_file}")
        except Exception as e:
            print(f"Warning: Could not read config file {config_file}. Using defaults. Error: {e}")
    else:
        print(f"Warning: Config file {config_file} not found. Using defaults.")

    # 読み込んだ設定を返す (適切な型変換を行う)
    settings = {
        'host': config['DEFAULT'].get('host'),
        'port': config['DEFAULT'].getint('port'), # 整数に変換
        'timeout': config['DEFAULT'].getint('timeout'),
        'max_retries': config['DEFAULT'].getint('max_retries'),
        'retry_delay': config['DEFAULT'].getfloat('retry_delay'), # 浮動小数点数に変換
        'log_level': config['DEFAULT'].get('log_level').upper(), # 大文字に
        'log_file': config['DEFAULT'].get('log_file') or None # 空文字列ならNone
    }
    return settings


class BlpApiWrapper:
    """
    Bloomberg API 非同期ラッパー (プロダクション向け強化版)
    - 詳細なエラーハンドリングとログ出力
    - 設定管理
    - Intradayデータ対応
    - スレッドセーフティ強化
    - Excelライクな簡易インターフェース
    """
    def __init__(self, config_file='config.ini'):
        self.settings = load_config(config_file)
        self.logger = setup_logger(
            level=getattr(logging, self.settings['log_level'], logging.INFO),
            log_file=self.settings['log_file']
        )

        self.host = self.settings['host']
        self.port = self.settings['port']
        self.default_timeout = self.settings['timeout']
        self.default_max_retries = self.settings['max_retries']
        self.default_retry_delay = self.settings['retry_delay']

        self.session = None
        self.service_states = {} # key: service_name, value: 'opening' or 'opened' or 'failed'
        self.service_states_lock = threading.Lock() # サービス状態辞書アクセス用ロック

        self.response_data = defaultdict(lambda: {"status": "pending", "data": [], "errors": []})
        self.response_data_lock = threading.Lock() # レスポンスデータ辞書アクセス用ロック

        self.event_thread = None
        self.shutdown_event = threading.Event()
        self.next_correlation_id = 0
        self.cid_lock = threading.Lock() # Correlation ID生成用ロック

        self.identity = None # 認証が必要な場合に使用

        self.logger.info(f"BlpApiWrapper initialized with settings: {self.settings}")

    def _get_next_correlation_id(self):
        with self.cid_lock:
            self.next_correlation_id += 1
            # CorrelationIdオブジェクトを作成
            return blpapi.CorrelationId(self.next_correlation_id)

    def start_session(self, auth_options=None):
        """
        セッションを開始し、イベント処理スレッドを起動。
        オプションで認証情報を提供可能。
        """
        if self.session:
            self.logger.warning("Session already started.")
            return True

        session_options = blpapi.SessionOptions()
        session_options.setServerHost(self.host)
        session_options.setServerPort(self.port)
        # session_options.setAutoRestartOnDisconnection(True) # 自動再接続オプション

        self.logger.info(f"Attempting to connect to {self.host}:{self.port}")
        # イベントハンドラを指定してセッション作成
        self.session = blpapi.Session(session_options, self._event_handler)

        if not self.session.start():
            self.logger.error("Failed to start session.")
            self.session = None
            return False

        self.logger.info("Session started successfully. Waiting for SessionStarted event...")

        # 認証処理 (必要な場合)
        if auth_options:
            self.logger.info("Attempting authorization...")
            if not self._authorize(auth_options):
                self.logger.error("Authorization failed.")
                self.stop_session()
                return False
            self.logger.info("Authorization successful.")
        else:
            self.logger.info("No authorization required or identity will be obtained later.")

        # イベント処理スレッドを開始
        self.shutdown_event.clear()
        self.event_thread = threading.Thread(target=self._event_loop, name="BlpapiEventThread")
        self.event_thread.daemon = True # メインスレッド終了時に自動終了させる場合
        self.event_thread.start()

        # SessionStarted イベントが来るまで少し待つ (堅牢にするならイベントで管理)
        time.sleep(1)

        self.logger.info("BlpApiWrapper session startup complete.")
        return True

    def _authorize(self, auth_options):
        """認証処理を実行"""
        if not self.session:
            self.logger.error("Session not started, cannot authorize.")
            return False

        # 認証用のCorrelation ID
        auth_cid = self.session.generateCorrelationId()

        # 認証サービスを開く
        if not self.session.openService("//blp/apiauth"):
            self.logger.error("Failed to open //blp/apiauth service.")
            return False
        auth_service = self.session.getService("//blp/apiauth")

        # 認証リクエスト作成
        auth_request = auth_service.createAuthorizationRequest()
        # auth_options の内容に応じて設定 (例: 'token', 'userAndApp', etc.)
        if 'token' in auth_options:
            auth_request.set("token", auth_options['token'])
        elif 'userAndApp' in auth_options:
             # User/App認証 (SAPI/B-PIPE)
             auth_info = self.session.AbstractSession.createIdentityUserInfo()
             auth_info.setUserName(auth_options['userAndApp'].get('user'))
             auth_info.setApplicationName(auth_options['userAndApp'].get('app'))
             # 必要に応じて他の情報も設定 (ipAddressなど)
             # 注意: createIdentityUserInfo の使用方法はバージョン等で異なる可能性あり
             # authorizationOptions として渡すのが最近の方法かもしれない
             self.logger.warning("User/App auth method might need review based on blpapi version.")
             # ここでは簡略化のため Identity を直接作成しない
             # SessionOptions に認証情報を設定する方が一般的
             self.logger.error("User/App auth via request is complex, prefer setting in SessionOptions.")
             return False # SessionOptionsでの設定を推奨

        else:
            self.logger.error(f"Unsupported auth_options: {auth_options}")
            return False

        # 認証リクエスト送信 (IdentityはまだないのでNone)
        self.session.sendAuthorizationRequest(auth_request, None, auth_cid)

        # 認証結果を待つ (イベントループで処理される)
        start_time = time.time()
        timeout = 30 # 認証タイムアウト
        while time.time() - start_time < timeout:
            event = self.session.nextEvent(timeout=1000) # 1秒待機
            if event.eventType() == blpapi.Event.RESPONSE or event.eventType() == blpapi.Event.REQUEST_STATUS:
                for msg in event:
                    if msg.correlationIds() and msg.correlationIds()[0] == auth_cid:
                        if msg.messageType() == blpapi.Name("AuthorizationSuccess"):
                            self.logger.info("AuthorizationSuccess received.")
                            # Identityを取得 (このIdentityを今後のリクエストで使用)
                            # 注意: Identityの取得方法は認証タイプにより異なる可能性あり
                            # Desktop APIでは通常Identityは不要 (自動)
                            # SAPI/B-PIPEではここでIdentityオブジェクトが取得できるはず
                            # self.identity = self.session.createIdentity() # Desktop APIなどでは不要なことが多い
                            self.logger.info("Identity established (details may vary by auth type).")
                            return True
                        elif msg.messageType() == blpapi.Name("AuthorizationFailure"):
                            self.logger.error(f"AuthorizationFailure received: {msg}")
                            # エラー詳細を取得
                            if msg.hasElement(REASON):
                                reason = msg.getElement(REASON)
                                self.logger.error(f"Reason: {reason.getElementAsString(DESCRIPTION)}")
                            return False
                        else:
                            self.logger.warning(f"Received unexpected message during auth: {msg.messageType()}")
            elif event.eventType() == blpapi.Event.TIMEOUT:
                 self.logger.warning("Timeout waiting for authorization response.")
                 continue # タイムアウトならループ継続
            else:
                 # 他の管理イベントなど
                 self.logger.debug(f"Received other event during auth: {event.eventType()}")
                 self._process_admin_or_status_event(event) # 他のイベントも処理しておく

        self.logger.error("Authorization timed out.")
        return False


    def stop_session(self):
        """セッションを停止し、スレッドを終了"""
        if not self.session:
            self.logger.info("Session already stopped or not started.")
            return

        self.logger.info("Stopping session...")
        self.shutdown_event.set() # イベントループに終了を通知

        # セッションを停止 (これによりイベントキューが解放される)
        # stop() は同期と非同期モードがある。ここでは同期的に停止を試みる。
        try:
            self.session.stop(blpapi.Session.StopOption.SYNC)
        except Exception as e:
            self.logger.error(f"Exception during session stop: {e}")
            # 非同期停止も試みる
            try:
                 self.session.stop(blpapi.Session.StopOption.ASYNC)
            except Exception as e_async:
                 self.logger.error(f"Exception during async session stop: {e_async}")


        # イベント処理スレッドの終了を待つ (タイムアウト付き)
        if self.event_thread and self.event_thread.is_alive():
            self.logger.info("Waiting for event thread to finish...")
            self.event_thread.join(timeout=5)
            if self.event_thread.is_alive():
                self.logger.warning("Event thread did not finish cleanly.")

        self.session = None
        self.service_states = {} # 状態をクリア
        self.identity = None
        self.logger.info("Session stopped.")

    def _open_service(self, service_name):
        """指定されたサービスを開く (同期的に完了を待つ、ロック使用)"""
        with self.service_states_lock:
            current_state = self.service_states.get(service_name)

            if current_state == 'opened':
                return True
            if current_state == 'opening':
                # 他のスレッドがオープン中の場合は待機
                pass # ロックがあるので待機は不要、後続の while ループで処理
            elif current_state == 'failed':
                 self.logger.warning(f"Service {service_name} previously failed to open. Retrying...")
                 # 再試行のために状態をリセット
                 del self.service_states[service_name]
                 current_state = None # ループに入るようにする
            # else: current_state is None (初回オープン)

        # サービスオープン開始 (ロック外)
        if not self.session:
            self.logger.error("Error: Session not started.")
            return False

        # 再度ロックを取得して状態を確認・設定
        with self.service_states_lock:
             # 他のスレッドが待機中にオープン完了したか、または失敗したか再チェック
            current_state = self.service_states.get(service_name)
            if current_state == 'opened':
                 return True
            if current_state == 'opening':
                 # まだ他のスレッドがオープン中 -> 待つ
                 pass
            elif current_state == 'failed':
                 # 待機中に失敗した -> False を返す
                 self.logger.error(f"Service {service_name} failed to open while waiting.")
                 return False
            else: # None または再試行
                 self.logger.info(f"Opening service: {service_name}")
                 self.service_states[service_name] = 'opening'
                 # サービスオープンリクエスト発行
                 if not self.session.openService(service_name):
                     self.logger.error(f"Failed to initiate opening service: {service_name}")
                     self.service_states[service_name] = 'failed'
                     return False

        # サービスが開くまで待機 (ロック外)
        timeout = 20 # サービスオープンタイムアウト
        start_time = time.time()
        opened_successfully = False
        while True:
            with self.service_states_lock:
                current_state = self.service_states.get(service_name)

            if current_state == 'opened':
                self.logger.info(f"Service {service_name} opened successfully.")
                opened_successfully = True
                break
            if current_state == 'failed':
                self.logger.error(f"Service {service_name} failed to open (detected state).")
                opened_successfully = False
                break
            if current_state != 'opening':
                self.logger.error(f"Unexpected service state '{current_state}' for {service_name} while waiting.")
                opened_successfully = False
                break

            if time.time() - start_time > timeout:
                self.logger.error(f"Timeout opening service {service_name} after {timeout} seconds.")
                with self.service_states_lock:
                     # タイムアウト時も状態を failed に設定
                     if self.service_states.get(service_name) == 'opening':
                         self.service_states[service_name] = 'failed'
                opened_successfully = False
                break

            # イベントが処理されるのを待つ
            time.sleep(0.1)

        return opened_successfully


    def _event_loop(self):
        """イベントキューを処理するループ (別スレッドで実行)"""
        self.logger.info("Event loop started.")
        while not self.shutdown_event.is_set():
            try:
                # イベントキューからイベントを取得 (タイムアウト付き)
                event = self.session.nextEvent(timeout=500) # 500ms timeout

                # イベントタイプに応じて処理を分岐
                if event.eventType() == blpapi.Event.TIMEOUT:
                    self.logger.debug("nextEvent timed out.")
                    continue
                elif event.eventType() in (PARTIAL_RESPONSE, RESPONSE):
                    # データレスポンスイベント
                    for msg in event:
                        self._process_response_message(msg, event.eventType())
                elif event.eventType() in (ADMIN_EVENT, SESSION_STATUS, SERVICE_STATUS):
                    # 管理イベントやステータスイベント
                    self._process_admin_or_status_event(event)
                else:
                    # 未知または無視するイベントタイプ
                    self.logger.debug(f"Ignoring event type: {event.eventType()}")
                    # メッセージ内容をデバッグログに出力する場合
                    # for msg in event:
                    #     self.logger.debug(f"Ignored message: {msg}")

            except Exception as e:
                # イベントループ内で予期せぬエラーが発生した場合
                self.logger.exception(f"Critical error in event loop: {e}")
                # 深刻な場合はセッション停止や再起動を検討
                # ここではループを継続するが、状況によっては終了させるべき
                time.sleep(1) # エラー発生時に無限ループを防ぐ

        self.logger.info("Event loop finished.")

    def _process_response_message(self, msg, event_type):
        """データリクエストに対するレスポンスメッセージを処理"""
        cids = msg.correlationIds()
        if not cids:
            self.logger.warning(f"Received response message with no Correlation ID: {msg.messageType()}")
            return

        # 通常、CIDは1つだが、複数含まれる可能性も考慮 (あまりない)
        for cid in cids:
            if not cid.isObject(): # User ObjectではなくValueの場合
                 cid_value = cid.value()
                 self.logger.debug(f"Processing response for CID: {cid_value}, EventType: {event_type}, MsgType: {msg.messageType()}")

                 with self.response_data_lock:
                    if cid_value not in self.response_data:
                        # タイムアウトなどで既に削除されたか、無効なCIDの可能性
                        self.logger.warning(f"Received response for unknown or outdated CID: {cid_value}. Message Type: {msg.messageType()}")
                        continue

                    # --- レスポンスデータの状態を取得 ---
                    current_response = self.response_data[cid_value]
                    # すでに完了 or エラー状態なら処理しない (重複メッセージなど)
                    if current_response["status"] in ["complete", "error"]:
                         self.logger.debug(f"Ignoring message for already completed/errored CID: {cid_value}")
                         continue


                    # --- メッセージ内容の解析 ---
                    has_request_level_error = False

                    # 1. リクエスト全体のエラーチェック
                    if msg.hasElement(RESPONSE_ERROR):
                        error_element = msg.getElement(RESPONSE_ERROR)
                        error_info = self._extract_error_info(error_element)
                        log_msg = f"CID {cid_value}: RequestError - Category: {error_info['category']}, SubCategory: {error_info['subcategory']}, Message: {error_info['message']}"
                        self.logger.error(log_msg)
                        current_response["errors"].append({"type": "RequestError", "details": error_info})
                        current_response["status"] = "error"
                        has_request_level_error = True

                    # RequestFailure メッセージタイプ
                    if msg.messageType() == REQUEST_FAILURE:
                         if msg.hasElement(REASON):
                             reason = msg.getElement(REASON)
                             error_info = self._extract_error_info(reason)
                             log_msg = f"CID {cid_value}: RequestFailure - Category: {error_info['category']}, SubCategory: {error_info['subcategory']}, Message: {error_info['message']}"
                             self.logger.error(log_msg)
                             current_response["errors"].append({"type": "RequestFailure", "details": error_info})
                             current_response["status"] = "error"
                             has_request_level_error = True
                         else:
                              log_msg = f"CID {cid_value}: RequestFailure received with no reason element."
                              self.logger.error(log_msg)
                              current_response["errors"].append({"type": "RequestFailure", "details": {"message": "No reason provided"}})
                              current_response["status"] = "error"
                              has_request_level_error = True

                    # リクエストレベルのエラーがなければ、データを抽出
                    if not has_request_level_error:
                        # 2. RefData / HistData のデータ抽出
                        if msg.hasElement(SECURITY_DATA):
                            security_data_array = msg.getElement(SECURITY_DATA)
                            for sec_data in security_data_array.values():
                                parsed_sec_data = self._parse_security_data(sec_data, cid_value)
                                current_response["data"].append(parsed_sec_data)
                                # 証券レベルやフィールドレベルのエラーもエラーリストに追加
                                current_response["errors"].extend(parsed_sec_data["errors"])

                        # 3. Intraday Bar データ抽出
                        elif msg.hasElement(BAR_DATA):
                            bar_data_element = msg.getElement(BAR_DATA)
                            if bar_data_element.hasElement(BAR_TICK_DATA):
                                bar_tick_data_array = bar_data_element.getElement(BAR_TICK_DATA)
                                for bar_tick in bar_tick_data_array.values():
                                    parsed_bar = self._parse_bar_tick_data(bar_tick, cid_value)
                                    current_response["data"].append(parsed_bar)
                            else:
                                self.logger.warning(f"CID {cid_value}: IntradayBarResponse received but no 'barTickData' element found.")

                        # 4. Intraday Tick データ抽出
                        elif msg.hasElement(TICK_DATA):
                            tick_data_element = msg.getElement(TICK_DATA)
                            if tick_data_element.hasElement(TICK_DATA): # ネストされている場合がある
                                tick_data_array = tick_data_element.getElement(TICK_DATA)
                                for tick in tick_data_array.values():
                                     parsed_tick = self._parse_tick_data(tick, cid_value)
                                     current_response["data"].append(parsed_tick)
                            else:
                                 self.logger.warning(f"CID {cid_value}: IntradayTickResponse received but no nested 'tickData' element found.")

                        # 5. BQLなどの他のレスポンスタイプ (将来用)
                        # elif msg.messageType() == blpapi.Name("BqlResponse"):
                        #     # BQL専用の解析ロジック
                        #     pass

                        else:
                            # 予期しないデータ構造
                            self.logger.warning(f"CID {cid_value}: Received response message with unknown data structure. MsgType: {msg.messageType()}")
                            # メッセージ内容をダンプ
                            # self.logger.debug(f"Unknown message content: {msg}")


                    # --- レスポンスステータスの更新 ---
                    # 既にエラー状態になっていなければ、イベントタイプに基づいて更新
                    if current_response["status"] != "error":
                        if event_type == RESPONSE:
                            current_response["status"] = "complete"
                            self.logger.info(f"CID {cid_value}: Response complete.")
                        elif event_type == PARTIAL_RESPONSE:
                            current_response["status"] = "partial"
                            self.logger.info(f"CID {cid_value}: Partial response received.")
                 # --- ロック解放 ---
            else:
                 # CIDがオブジェクトの場合 (認証など特別なケース)
                 self.logger.debug(f"Ignoring message with object Correlation ID in response processor: {cid}")

    def _extract_error_info(self, error_element):
        """エラー要素から情報を抽出するヘルパー"""
        info = {
            "category": error_element.getElementAsString(CATEGORY) if error_element.hasElement(CATEGORY) else "UNKNOWN",
            "subcategory": error_element.getElementAsString(SUBCATEGORY) if error_element.hasElement(SUBCATEGORY) else "UNKNOWN",
            "message": error_element.getElementAsString(MESSAGE) if error_element.hasElement(MESSAGE) else "No message",
            "code": error_element.getElementAsInteger(CODE) if error_element.hasElement(CODE) else -1,
            "source": error_element.getElementAsString(SOURCE) if error_element.hasElement(SOURCE) else "UNKNOWN",
        }
        # エラー要素全体もログ用に保持しても良いかも
        # info["raw"] = str(error_element)
        return info

    def _parse_security_data(self, sec_data, cid_value):
        """securityData 要素を解析 (RefData/HistData)"""
        result = {"security": "UNKNOWN", "data": {}, "errors": []}
        if sec_data.hasElement(SECURITY_NAME):
            result["security"] = sec_data.getElementAsString(SECURITY_NAME)
        sec_name = result["security"] # ログ用

        # 証券レベルのエラーチェック
        if sec_data.hasElement(SECURITY_ERROR):
            error_element = sec_data.getElement(SECURITY_ERROR)
            error_info = self._extract_error_info(error_element)
            log_msg = f"CID {cid_value}, Security {sec_name}: SecurityError - Category: {error_info['category']}, Message: {error_info['message']}"
            self.logger.warning(log_msg)
            result["errors"].append({"type": "SecurityError", "details": error_info})

        # フィールドデータ抽出 (HistDataの場合は配列、RefDataの場合は辞書的)
        if sec_data.hasElement(FIELD_DATA):
            field_data = sec_data.getElement(FIELD_DATA)
            if field_data.isArray(): # HistoricalDataResponse
                result["data"] = [] # リストとして初期化
                for daily_data in field_data.values():
                    day_result = {}
                    for field in daily_data.elements():
                        field_name = str(field.name())
                        try:
                             # 型に応じて適切な getValueAs を呼ぶ (ここでは単純化)
                             if field.datatype() == blpapi.DataType.FLOAT64 or field.datatype() == blpapi.DataType.FLOAT32:
                                 day_result[field_name] = field.getValueAsFloat()
                             elif field.datatype() == blpapi.DataType.INT64 or field.datatype() == blpapi.DataType.INT32:
                                 day_result[field_name] = field.getValueAsInteger()
                             elif field.datatype() == blpapi.DataType.DATE or field.datatype() == blpapi.DataType.DATETIME:
                                  # datetimeオブジェクトに変換 (タイムゾーン考慮が必要な場合あり)
                                  dt_val = field.getValueAsDatetime()
                                  # naiveなdatetimeかもしれないので注意
                                  day_result[field_name] = dt_val # .isoformat() など文字列にしても良い
                             else:
                                 day_result[field_name] = field.getValueAsString()
                        except Exception as e:
                             self.logger.warning(f"CID {cid_value}, Sec {sec_name}: Error parsing field '{field_name}': {e}. Using string representation.")
                             day_result[field_name] = field.getValueAsString() # エラー時は文字列で取得試行
                    result["data"].append(day_result)
            else: # ReferenceDataResponse
                result["data"] = {} # 辞書として初期化
                for field in field_data.elements():
                    field_name = str(field.name())
                    try:
                        if field.datatype() == blpapi.DataType.FLOAT64 or field.datatype() == blpapi.DataType.FLOAT32:
                            result["data"][field_name] = field.getValueAsFloat()
                        elif field.datatype() == blpapi.DataType.INT64 or field.datatype() == blpapi.DataType.INT32:
                            result["data"][field_name] = field.getValueAsInteger()
                        elif field.datatype() == blpapi.DataType.DATE or field.datatype() == blpapi.DataType.DATETIME:
                             dt_val = field.getValueAsDatetime()
                             result["data"][field_name] = dt_val #.isoformat()
                        elif field.datatype() == blpapi.DataType.STRING:
                             result["data"][field_name] = field.getValueAsString()
                        elif field.isArray(): # BDSのような配列データ
                             # 配列内の要素を処理 (さらにネストする可能性も)
                             array_values = []
                             for item in field.values():
                                  # item がさらに要素を持つ場合 (Bulk Field)
                                  if item.numElements() > 0:
                                       item_data = {}
                                       for sub_element in item.elements():
                                            sub_name = str(sub_element.name())
                                            # 再帰的に値を取得するヘルパー関数があると良いかも
                                            try:
                                                item_data[sub_name] = sub_element.getValueAsString() # ここも型判定すべき
                                            except:
                                                item_data[sub_name] = "[Error Parsing]"
                                       array_values.append(item_data)
                                  else:
                                       try:
                                            array_values.append(item.getValueAsString()) # 単純な配列要素
                                       except:
                                            array_values.append("[Error Parsing]")
                             result["data"][field_name] = array_values
                        else:
                            result["data"][field_name] = field.getValueAsString()
                    except Exception as e:
                        self.logger.warning(f"CID {cid_value}, Sec {sec_name}: Error parsing field '{field_name}': {e}. Using string representation.")
                        result["data"][field_name] = field.getValueAsString()

        # フィールドレベルの例外チェック
        if sec_data.hasElement(FIELD_EXCEPTIONS):
            field_exceptions = sec_data.getElement(FIELD_EXCEPTIONS)
            for ex in field_exceptions.values():
                field_id = ex.getElementAsString(FIELD_ID)
                error_info = self._extract_error_info(ex.getElement(ERROR_INFO))
                log_msg = f"CID {cid_value}, Security {sec_name}, Field {field_id}: FieldError - Category: {error_info['category']}, Message: {error_info['message']}"
                self.logger.warning(log_msg)
                result["errors"].append({"type": "FieldError", "field": field_id, "details": error_info})

        return result

    def _parse_bar_tick_data(self, bar_tick, cid_value):
        """barTickData 要素を解析 (IntradayBar)"""
        data = {}
        try:
            # datetimeオブジェクトとして取得 (UTCのはず)
            data['time'] = bar_tick.getElementAsDatetime(TIME) if bar_tick.hasElement(TIME) else None
            data['open'] = bar_tick.getElementAsFloat(OPEN) if bar_tick.hasElement(OPEN) else None
            data['high'] = bar_tick.getElementAsFloat(HIGH) if bar_tick.hasElement(HIGH) else None
            data['low'] = bar_tick.getElementAsFloat(LOW) if bar_tick.hasElement(LOW) else None
            data['close'] = bar_tick.getElementAsFloat(CLOSE) if bar_tick.hasElement(CLOSE) else None
            data['volume'] = bar_tick.getElementAsInteger(VOLUME) if bar_tick.hasElement(VOLUME) else None
            data['numEvents'] = bar_tick.getElementAsInteger(NUM_EVENTS) if bar_tick.hasElement(NUM_EVENTS) else None
            # 他に必要な要素があれば追加
        except Exception as e:
            self.logger.error(f"CID {cid_value}: Error parsing Intraday Bar data: {e}. Raw: {bar_tick}")
            # エラー発生時は部分的なデータやNoneを返す
        return data

    def _parse_tick_data(self, tick, cid_value):
        """tickData 要素を解析 (IntradayTick)"""
        data = {}
        try:
             # datetimeオブジェクトとして取得 (UTCのはず)
            data['time'] = tick.getElementAsDatetime(TIME) if tick.hasElement(TIME) else None
            data['type'] = tick.getElementAsString(TYPE) if tick.hasElement(TYPE) else None # 例: "TRADE", "BID", "ASK"
            data['value'] = tick.getElementAsFloat(VALUE) if tick.hasElement(VALUE) else None # 価格
            # size, conditionCodes など他の要素も必要に応じて追加
            # data['size'] = tick.getElementAsInteger("size") if tick.hasElement("size") else None
            # if tick.hasElement("conditionCodes"):
            #     data['conditionCodes'] = tick.getElementAsString("conditionCodes")

        except Exception as e:
            self.logger.error(f"CID {cid_value}: Error parsing Intraday Tick data: {e}. Raw: {tick}")
        return data


    def _process_admin_or_status_event(self, event):
        """管理イベントやステータスイベントを処理"""
        for msg in event:
            msg_type = msg.messageType()
            self.logger.debug(f"Processing Admin/Status Event: Type={event.eventType()}, MsgType={msg_type}")

            if msg_type == SESSION_STARTED:
                self.logger.info("SessionStarted event processed.")
            elif msg_type == SESSION_STARTUP_FAILURE:
                self.logger.error(f"SessionStartupFailure event: {msg}")
                # 深刻なエラーなのでセッション停止を試みるべきか？
            elif msg_type == SESSION_TERMINATED:
                self.logger.warning(f"SessionTerminated event: {msg}")
                # セッションが予期せず終了した場合の処理
                # 再接続ロジックなどが必要ならここから起動
                self.shutdown_event.set() # イベントループを止める
            elif msg_type == SERVICE_OPENED:
                service_name = msg.getElementAsString("serviceName")
                with self.service_states_lock:
                    self.service_states[service_name] = 'opened'
                self.logger.info(f"Service opened event processed: {service_name}")
            elif msg_type == SERVICE_OPEN_FAILURE:
                service_name = msg.getElementAsString("serviceName")
                with self.service_states_lock:
                    self.service_states[service_name] = 'failed'
                self.logger.error(f"Service open failure event processed: {service_name}, Reason: {msg}")
            elif msg_type == SLOW_CONSUMER_WARNING:
                self.logger.warning("SlowConsumerWarning event received. Event queue may be backing up.")
            elif msg_type == SLOW_CONSUMER_WARNING_CLEARED:
                 self.logger.info("SlowConsumerWarningCleared event received.")
            elif msg_type == DATA_LOSS:
                service_name = msg.getElementAsString("serviceName")
                num_messages_lost = msg.getElementAsInteger("numMessagesLost")
                self.logger.critical(f"DataLoss event received for service {service_name}! Lost {num_messages_lost} messages.")
                # データロスは深刻。対応が必要。
            # 他の重要な管理/ステータスメッセージがあれば処理を追加
            # 例: PermissionRequest, ResolutionSuccess/Failure など


    def send_request(self, request, service_name,
                     timeout=None, max_retries=None, retry_delay=None):
        """
        リクエストを送信し、完了またはタイムアウト/エラーまで待機して結果を返す。
        リトライ機能付き。設定値がなければデフォルト値を使用。
        """
        if not self.session:
            self.logger.error("Session not started. Cannot send request.")
            # ConnectionError を raise するか、エラー辞書を返すか選択
            return {"status": "error", "data": [], "errors": [{"type": "SessionError", "details": {"message": "Session not started."}}]}

        # パラメータがNoneの場合はデフォルト値を使用
        timeout = timeout if timeout is not None else self.default_timeout
        max_retries = max_retries if max_retries is not None else self.default_max_retries
        retry_delay = retry_delay if retry_delay is not None else self.default_retry_delay

        # サービスがオープンしているか確認・試行
        if not self._open_service(service_name):
            err_msg = f"Failed to open required service: {service_name}"
            self.logger.error(err_msg)
            return {"status": "error", "data": [], "errors": [{"type": "ServiceError", "details": {"message": err_msg}}]}

        # サービスオブジェクト取得
        try:
            service = self.session.getService(service_name)
            if not service:
                 # _open_service が True でも稀に None になるケースがある？念のためチェック
                 raise ValueError(f"Could not get service object for {service_name} even after successful open.")
        except Exception as e:
             err_msg = f"Failed to get service object for {service_name}: {e}"
             self.logger.exception(err_msg) # スタックトレースも記録
             return {"status": "error", "data": [], "errors": [{"type": "ServiceError", "details": {"message": err_msg}}]}


        cid = self._get_next_correlation_id()
        cid_value = cid.value()
        last_exception = None
        current_retry_delay = retry_delay

        for attempt in range(max_retries + 1):
            self.logger.info(f"Sending request CID: {cid_value}, Service: {service_name}, Attempt {attempt + 1}/{max_retries + 1}")
            # self.logger.debug(f"Request details CID {cid_value}: {request}") # 必要ならリクエスト内容もログに

            # 各試行でレスポンス状態を初期化 (ロック内で)
            with self.response_data_lock:
                self.response_data[cid_value] = {"status": "pending", "data": [], "errors": []}

            # リクエスト送信
            try:
                self.session.sendRequest(request=request, correlationId=cid, identity=self.identity) # Identity を使用
            except Exception as e:
                self.logger.exception(f"Failed to send request (CID: {cid_value}, Attempt {attempt + 1}): {e}")
                last_exception = e
                if attempt < max_retries:
                    self.logger.warning(f"Retrying send request in {current_retry_delay:.1f} seconds...")
                    time.sleep(current_retry_delay)
                    current_retry_delay *= 2 # 指数バックオフ
                    continue
                else:
                    # 最大リトライ回数に達したらエラーを返す
                    err_msg = f"Failed to send request after {max_retries + 1} attempts: {e}"
                    with self.response_data_lock:
                        # 念のため最終状態を設定してから pop
                        self.response_data[cid_value]["status"] = "error"
                        self.response_data[cid_value]["errors"].append({"type": "SendRequestError", "details": {"message": err_msg}})
                        result = self.response_data.pop(cid_value)
                    return result

            # --- 完了待機 (タイムアウトあり) ---
            start_time = time.time()
            wait_outcome = "pending" # "complete", "error", "timeout"
            while True:
                with self.response_data_lock:
                    current_status = self.response_data.get(cid_value, {}).get("status", "unknown")

                if current_status == "complete":
                    self.logger.info(f"CID {cid_value}: Request completed (Attempt {attempt + 1}).")
                    wait_outcome = "complete"
                    break
                if current_status == "error":
                    self.logger.warning(f"CID {cid_value}: Request ended with error status (Attempt {attempt + 1}).")
                    wait_outcome = "error"
                    break
                if current_status not in ["pending", "partial"]:
                     self.logger.error(f"CID {cid_value}: Unexpected status '{current_status}' while waiting.")
                     wait_outcome = "error" # 予期せぬ状態はエラー扱い
                     # 状態を強制的にエラーにする
                     with self.response_data_lock:
                          if cid_value in self.response_data:
                               self.response_data[cid_value]["status"] = "error"
                               self.response_data[cid_value]["errors"].append({"type": "InternalError", "details": {"message": f"Unexpected status '{current_status}'"}})
                     break

                if time.time() - start_time > timeout:
                    self.logger.warning(f"Request timed out (CID: {cid_value}, Attempt {attempt + 1}) after {timeout} seconds.")
                    wait_outcome = "timeout"
                    last_exception = TimeoutError(f"Request timed out after {timeout}s")
                    # タイムアウト時も状態をエラーにする
                    with self.response_data_lock:
                         if cid_value in self.response_data:
                              self.response_data[cid_value]["status"] = "error"
                              self.response_data[cid_value]["errors"].append({"type": "Timeout", "details": {"message": f"Request timed out after {timeout}s"}})
                    break

                # イベントが処理されるのを待つ
                time.sleep(0.05) # 待ち時間を少し短く

            # --- 待機結果に基づく処理 ---
            # 結果を取得 (ロック内で)
            with self.response_data_lock:
                 result = self.response_data.pop(cid_value, None) # popして取得

            if not result:
                  # pop に失敗した場合 (通常は起こらないはず)
                  err_msg = f"Internal Error: Response data for CID {cid_value} disappeared after waiting."
                  self.logger.error(err_msg)
                  # リトライを試みるか？ここでは最終エラーとする
                  last_exception = RuntimeError(err_msg)
                  if attempt < max_retries:
                       self.logger.warning(f"Retrying due to missing response data in {current_retry_delay:.1f} seconds...")
                       time.sleep(current_retry_delay)
                       current_retry_delay *= 2
                       continue
                  else:
                       return {"status": "error", "data": [], "errors": [{"type": "InternalError", "details": {"message": err_msg + f" after {max_retries + 1} attempts."}}]}


            # 待機結果がタイムアウトまたはエラーの場合、リトライ判断
            if wait_outcome == "timeout" or wait_outcome == "error":
                # リトライすべきエラーか判断
                should_retry = False
                if wait_outcome == "timeout":
                    # タイムアウトは基本的にリトライ対象
                    should_retry = True
                    self.logger.warning(f"CID {cid_value}: Retrying due to timeout.")
                else: # wait_outcome == "error"
                    # エラー内容をチェックしてリトライ判断
                    for error in result.get("errors", []):
                        error_details = error.get("details", {})
                        category = error_details.get("category", "UNKNOWN").upper()
                        subcategory = error_details.get("subcategory", "UNKNOWN").upper()
                        # リトライ可能なカテゴリ/サブカテゴリか？ (要調整)
                        if category in RETRYABLE_CATEGORIES or "TIMEOUT" in category or "CONNECTION" in category:
                            # ただし、明確にリトライ不可なエラーは除く
                            if category not in NON_RETRYABLE_CATEGORIES and subcategory not in NON_RETRYABLE_CATEGORIES:
                                should_retry = True
                                last_exception = RuntimeError(f"Retrying due to potentially recoverable error: Category={category}, SubCategory={subcategory}, Message={error_details.get('message', 'N/A')}")
                                self.logger.warning(f"CID {cid_value}: Detected potentially recoverable error, will retry. Details: {error_details}")
                                break # リトライ決定

                if should_retry and attempt < max_retries:
                    self.logger.info(f"Retrying request CID {cid_value} in {current_retry_delay:.1f} seconds...")
                    time.sleep(current_retry_delay)
                    current_retry_delay *= 2 # 指数バックオフ
                    continue # 次の試行へ
                elif should_retry and attempt == max_retries:
                    self.logger.error(f"Max retries ({max_retries + 1}) reached for CID {cid_value} after encountering retryable error/timeout.")
                    result["errors"].append({"type": "MaxRetriesExceeded", "details": {"message": f"Failed after {max_retries + 1} attempts. Last error: {last_exception}"}})
                    result["status"] = "error" # 最終状態はエラー
                    return result
                else: # リトライ不要なエラー or 最大リトライ回数超過
                    self.logger.info(f"CID {cid_value}: Request finished with unrecoverable error or completed with errors. Status: {result['status']}")
                    return result # リトライしないので結果を返す

            elif wait_outcome == "complete":
                 # 正常完了 (エラーが含まれる可能性はあるが、リトライ対象ではない)
                 self.logger.info(f"CID {cid_value}: Request completed successfully (Attempt {attempt + 1}).")
                 return result

        # ループが正常に終了した場合 (通常ここには来ないはず)
        self.logger.error(f"CID {cid_value}: Request loop exited unexpectedly.")
        return {"status": "error", "data": [], "errors": [{"type": "Unknown", "details": {"message": "Reached end of request loop unexpectedly"}}]}


    # --- Excelライクな高レベルメソッド ---

    def bdp(self, securities, fields, overrides=None, timeout=None, max_retries=None, retry_delay=None):
        """
        ReferenceDataRequest を使って静的データを取得 (ExcelのBDP相当)。
        :param securities: 証券名のリスト (例: ["IBM US Equity", "MSFT US Equity"])
        :param fields: フィールド名のリスト (例: ["PX_LAST", "BID", "ASK"])
        :param overrides: オーバーライド辞書 (例: {"VWAP_START_TIME": "9:30", "VWAP_END_TIME": "16:00"})
        :return: 結果辞書 {"status": "complete"|"error", "data": [...], "errors": [...]}
                 data は証券ごとの辞書のリスト: [{"security": "...", "data": {"FIELD": value, ...}, "errors": [...]}, ...]
        """
        self.logger.info(f"Received BDP request for {len(securities)} securities, {len(fields)} fields.")
        service_name = "//blp/refdata"
        try:
            # サービスオブジェクトの取得を試みる (内部でオープンされる)
            service = self.session.getService(service_name)
            if not service:
                 raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("ReferenceDataRequest")

            # 引数をリストで受け付けるように統一
            if isinstance(securities, str): securities = [securities]
            if isinstance(fields, str): fields = [fields]

            for sec in securities:
                request.append("securities", sec)
            for fld in fields:
                request.append("fields", fld)

            if overrides:
                override_element = request.getElement("overrides")
                for key, value in overrides.items():
                    ovrd = override_element.appendElement()
                    ovrd.setElement("fieldId", key)
                    ovrd.setElement("value", str(value)) # 値は文字列として設定するのが安全

            return self.send_request(request, service_name, timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating BDP request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


    def bdh(self, securities, fields, start_date, end_date, overrides=None, options=None,
            timeout=None, max_retries=None, retry_delay=None):
        """
        HistoricalDataRequest を使って時系列データを取得 (ExcelのBDH相当)。
        :param securities: 証券名のリスト
        :param fields: フィールド名のリスト
        :param start_date: 開始日 ("YYYYMMDD" または datetime.date)
        :param end_date: 終了日 ("YYYYMMDD" または datetime.date)
        :param overrides: オーバーライド辞書
        :param options: その他のオプション辞書 (例: {"periodicitySelection": "DAILY", "currency": "USD"})
        :return: 結果辞書 {"status": "complete"|"error", "data": [...], "errors": [...]}
                 data は証券ごとの辞書のリスト: [{"security": "...", "data": [{"date": ..., "FIELD": value, ...}, ...], "errors": [...]}, ...]
        """
        self.logger.info(f"Received BDH request for {len(securities)} securities, {len(fields)} fields from {start_date} to {end_date}.")
        service_name = "//blp/refdata"
        try:
            service = self.session.getService(service_name)
            if not service:
                 raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("HistoricalDataRequest")

            if isinstance(securities, str): securities = [securities]
            if isinstance(fields, str): fields = [fields]

            for sec in securities:
                request.append("securities", sec)
            for fld in fields:
                request.append("fields", fld)

            # 日付形式の統一 (YYYYMMDD)
            if isinstance(start_date, datetime): start_date = start_date.strftime("%Y%m%d")
            if isinstance(end_date, datetime): end_date = end_date.strftime("%Y%m%d")
            request.set("startDate", start_date)
            request.set("endDate", end_date)

            # その他のオプション設定
            if options:
                for key, value in options.items():
                    try:
                        if request.hasElement(key):
                            request.set(key, value)
                        else:
                            self.logger.warning(f"BDH: Option '{key}' not found in request schema. Ignoring.")
                    except Exception as e_opt:
                        self.logger.warning(f"BDH: Failed to set option '{key}' to '{value}'. Error: {e_opt}")


            if overrides:
                override_element = request.getElement("overrides")
                for key, value in overrides.items():
                    ovrd = override_element.appendElement()
                    ovrd.setElement("fieldId", key)
                    ovrd.setElement("value", str(value))

            # 時系列データはタイムアウトを長めに設定する (例: デフォルトの2倍)
            hist_timeout = timeout if timeout is not None else self.default_timeout * 2
            return self.send_request(request, service_name, hist_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating BDH request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


    def bds(self, securities, field, overrides=None, options=None,
             timeout=None, max_retries=None, retry_delay=None):
        """
        ReferenceDataRequest を使ってバルクデータを取得 (ExcelのBDS相当)。
        特定のフィールドに対する配列データを取得することを想定。
        :param securities: 証券名のリスト
        :param field: バルクフィールド名 (文字列、1つだけ)
        :param overrides: オーバーライド辞書
        :param options: ReferenceDataRequestの他のオプション (あまり使わないかも)
        :return: 結果辞書 {"status": "complete"|"error", "data": [...], "errors": [...]}
                 data は証券ごとの辞書のリスト: [{"security": "...", "data": {"FIELD": [item1, item2, ...]}, "errors": [...]}, ...]
                 item は単純な値か、ネストした辞書。
        """
        self.logger.info(f"Received BDS request for {len(securities)} securities, field: {field}.")
        service_name = "//blp/refdata"
        try:
            service = self.session.getService(service_name)
            if not service:
                 raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("ReferenceDataRequest")

            if isinstance(securities, str): securities = [securities]
            if not isinstance(field, str):
                 raise ValueError("BDS function requires a single field name (string).")

            for sec in securities:
                request.append("securities", sec)
            request.append("fields", field) # フィールドは1つ

            # その他のオプション設定 (必要であれば)
            if options:
                 for key, value in options.items():
                     try:
                         if request.hasElement(key):
                             request.set(key, value)
                         else:
                              self.logger.warning(f"BDS: Option '{key}' not found in request schema. Ignoring.")
                     except Exception as e_opt:
                         self.logger.warning(f"BDS: Failed to set option '{key}' to '{value}'. Error: {e_opt}")

            if overrides:
                override_element = request.getElement("overrides")
                for key, value in overrides.items():
                    ovrd = override_element.appendElement()
                    ovrd.setElement("fieldId", key)
                    ovrd.setElement("value", str(value))

            # BDSもデータ量が多い可能性があるのでタイムアウト長め推奨
            bds_timeout = timeout if timeout is not None else self.default_timeout * 2
            return self.send_request(request, service_name, bds_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating BDS request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


    def get_intraday_bar(self, security, event_type, start_dt, end_dt, interval,
                         options=None, timeout=None, max_retries=None, retry_delay=None):
        """
        IntradayBarRequest を使って分足などを取得。
        :param security: 証券名 (文字列、1つだけ)
        :param event_type: イベントタイプ ("TRADE", "BID", "ASK", "BID_BEST", "ASK_BEST", etc.)
        :param start_dt: 開始日時 (datetimeオブジェクト, timezone-aware推奨)
        :param end_dt: 終了日時 (datetimeオブジェクト, timezone-aware推奨)
        :param interval: 間隔（分単位の整数）
        :param options: その他のオプション辞書 (例: {"gapFillInitialBar": True})
        :return: 結果辞書 {"status": "complete"|"error", "data": [...], "errors": [...]}
                 data はバーデータのリスト: [{"time": ..., "open": ..., "high": ..., ...}, ...]
                 エラーはリストのerrorsに含まれる。Intradayは証券ごとのエラー構造が少し違う。
        """
        self.logger.info(f"Received IntradayBar request for {security}, Event: {event_type}, Interval: {interval}, Period: {start_dt} to {end_dt}")
        service_name = "//blp/apidata" # Intradayデータは通常このサービス
        try:
            service = self.session.getService(service_name)
            if not service:
                 raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("IntradayBarRequest")

            request.set("security", security)
            request.set("eventType", event_type)
            request.set("interval", interval)

            # datetime を blpapi が期待する形式に設定
            # blpapi.Datetime オブジェクトを使うのが確実
            blp_start_dt = blpapi.Datetime.from_datetime(start_dt)
            blp_end_dt = blpapi.Datetime.from_datetime(end_dt)
            request.set("startDateTime", blp_start_dt)
            request.set("endDateTime", blp_end_dt)

            # その他のオプション設定
            if options:
                for key, value in options.items():
                    try:
                        if request.hasElement(key):
                            request.set(key, value)
                        else:
                             self.logger.warning(f"IntradayBar: Option '{key}' not found in request schema. Ignoring.")
                    except Exception as e_opt:
                        self.logger.warning(f"IntradayBar: Failed to set option '{key}' to '{value}'. Error: {e_opt}")


            # Intradayデータは非常に大きい可能性があるのでタイムアウトを長く設定
            intra_timeout = timeout if timeout is not None else self.default_timeout * 4
            return self.send_request(request, service_name, intra_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating IntradayBar request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}


    def get_intraday_tick(self, security, event_types, start_dt, end_dt,
                          options=None, timeout=None, max_retries=None, retry_delay=None):
        """
        IntradayTickRequest を使ってティックデータを取得。
        :param security: 証券名 (文字列、1つだけ)
        :param event_types: イベントタイプのリスト (例: ["TRADE", "BID", "ASK"])
        :param start_dt: 開始日時 (datetimeオブジェクト, timezone-aware推奨)
        :param end_dt: 終了日時 (datetimeオブジェクト, timezone-aware推奨)
        :param options: その他のオプション辞書 (例: {"includeConditionCodes": True})
        :return: 結果辞書 {"status": "complete"|"error", "data": [...], "errors": [...]}
                 data はティックデータのリスト: [{"time": ..., "type": ..., "value": ..., ...}, ...]
        """
        self.logger.info(f"Received IntradayTick request for {security}, Events: {event_types}, Period: {start_dt} to {end_dt}")
        service_name = "//blp/apidata"
        try:
            service = self.session.getService(service_name)
            if not service:
                 raise ConnectionError(f"Could not get service: {service_name}")

            request = service.createRequest("IntradayTickRequest")

            request.set("security", security)

            # イベントタイプは配列で設定
            if isinstance(event_types, str): event_types = [event_types]
            for et in event_types:
                request.append("eventTypes", et)

            blp_start_dt = blpapi.Datetime.from_datetime(start_dt)
            blp_end_dt = blpapi.Datetime.from_datetime(end_dt)
            request.set("startDateTime", blp_start_dt)
            request.set("endDateTime", blp_end_dt)

            # その他のオプション設定
            if options:
                 for key, value in options.items():
                     try:
                         if request.hasElement(key):
                             request.set(key, value)
                         else:
                              self.logger.warning(f"IntradayTick: Option '{key}' not found in request schema. Ignoring.")
                     except Exception as e_opt:
                         self.logger.warning(f"IntradayTick: Failed to set option '{key}' to '{value}'. Error: {e_opt}")


            intra_timeout = timeout if timeout is not None else self.default_timeout * 5 # Tickはさらに時間がかかる可能性
            return self.send_request(request, service_name, intra_timeout, max_retries, retry_delay)

        except Exception as e:
            self.logger.exception(f"Error creating IntradayTick request: {e}")
            return {"status": "error", "data": [], "errors": [{"type": "RequestCreationError", "details": {"message": str(e)}}]}

    def __enter__(self):
        """Context Manager: セッションを開始"""
        if not self.start_session():
            raise RuntimeError("Failed to start Bloomberg session.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context Manager: セッションを停止"""
        self.stop_session()


# --- asyncio との連携 ---
async def run_blp_request_async(blp_wrapper, request_func, *args, **kwargs):
    """
    BlpApiWrapper の同期メソッドを asyncio イベントループから非同期に呼び出すヘルパー関数。
    Python 3.9+ の asyncio.to_thread を使用。
    """
    loop = asyncio.get_running_loop()
    # 同期関数 request_func を別スレッドで実行し、結果を await する
    result = await asyncio.to_thread(request_func, *args, **kwargs)
    # 低レベルの asyncio.run_in_executor を使う場合:
    # result = await loop.run_in_executor(None, request_func, *args, **kwargs) # デフォルトのThreadPoolExecutorを使用
    return result

# --- 利用例 ---
async def main_async_example():
    """asyncioを使った非同期実行の例"""
    blp = BlpApiWrapper() # ラッパーインスタンス作成
    if not blp.start_session(): # セッション開始は同期的に行う
        print("Failed to start session.")
        return

    try:
        # --- 複数のリクエストを非同期に発行 ---
        print("\n--- Sending requests asynchronously using asyncio ---")

        # BDPリクエストのコルーチンを作成
        bdp_task = run_blp_request_async(
            blp,
            blp.bdp, # 呼び出すメソッド
            ["IBM US Equity", "MSFT US Equity", "INVALID SEC"], # securities
            ["PX_LAST", "SECURITY_NAME_REALTIME"] # fields
        )

        # BDHリクエストのコルーチンを作成
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 5)
        bdh_task = run_blp_request_async(
            blp,
            blp.bdh, # 呼び出すメソッド
            "AAPL US Equity", # securities
            ["PX_LAST", "VOLUME"], # fields
            start_date.strftime("%Y%m%d"), # start_date
            end_date.strftime("%Y%m%d"), # end_date
            options={"periodicitySelection": "DAILY"}
        )

        # 他の非同期タスク (例: 別のDBアクセスラッパー)
        db_task = asyncio.sleep(1, result="DB result placeholder") # ダミーDBアクセス

        # すべてのタスクを並行して実行し、結果を待つ
        results = await asyncio.gather(bdp_task, bdh_task, db_task)

        # 結果の処理
        bdp_result_async = results[0]
        bdh_result_async = results[1]
        db_result = results[2]

        print("\n--- Async BDP Result ---")
        print(json.dumps(bdp_result_async, indent=2, default=str)) # datetimeを文字列化

        print("\n--- Async BDH Result ---")
        print(json.dumps(bdh_result_async, indent=2, default=str))

        print(f"\n--- Other Async Task Result ---")
        print(db_result)

    except Exception as e:
        print(f"An error occurred in async example: {e}")
        blp.logger.exception("Error during async execution")
    finally:
        blp.stop_session() # セッション停止も忘れずに


if __name__ == '__main__':
    # --- 同期的な使い方 ---
    print("--- Synchronous Example ---")
    try:
        # コンテキストマネージャを使ってセッション管理
        with BlpApiWrapper(config_file='config.ini') as blp:

            # BDP Example
            print("\n--- BDP Call ---")
            bdp_result = blp.bdp(
                securities=["NVDA US Equity", "AMD US Equity", "NONEXISTENT"],
                fields=["PX_LAST", "CHG_PCT_1D", "BEST_EPS"],
                overrides={"BEST_FPERIOD_OVERRIDE": "1BF"} # 例: 次の決算期
            )
            print("BDP Result:")
            # datetime オブジェクトは json.dumps でエラーになるので default=str を指定
            print(json.dumps(bdp_result, indent=2, default=str))

            # BDH Example
            print("\n--- BDH Call ---")
            bdh_result = blp.bdh(
                securities="GOOGL UW Equity", # 文字列でもリストでもOK
                fields=["PX_LAST", "TURNOVER"],
                start_date="20240101",
                end_date="20240105",
                options={"periodicitySelection": "DAILY", "nonTradingDayFillOption": "ALL_CALENDAR_DAYS"}
            )
            print("BDH Result:")
            print(json.dumps(bdh_result, indent=2, default=str))

            # BDS Example
            print("\n--- BDS Call ---")
            bds_result = blp.bds(
                securities="INDU Index",
                field="INDX_MWEIGHT", # 指数構成銘柄ウェイト (バルクフィールド)
                overrides={"END_DATE_OVERRIDE": "20240131"}
            )
            print("BDS Result:")
            print(json.dumps(bds_result, indent=2, default=str))

            # Intraday Bar Example
            # print("\n--- Intraday Bar Call ---")
            # now = datetime.now()
            # start_dt_bar = now - timedelta(hours=1)
            # end_dt_bar = now
            # intraday_bar_result = blp.get_intraday_bar(
            #     security="MSFT US Equity",
            #     event_type="TRADE",
            #     start_dt=start_dt_bar,
            #     end_dt=end_dt_bar,
            #     interval=5 # 5分足
            # )
            # print("Intraday Bar Result:")
            # print(json.dumps(intraday_bar_result, indent=2, default=str))

    except RuntimeError as e:
        print(f"Runtime Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logging.getLogger('blpapi_wrapper').exception("Unhandled error in main execution")


    # --- 非同期 (asyncio) の使い方 ---
    print("\n\n--- Asynchronous Example using asyncio ---")
    # asyncio イベントループを実行
    try:
        # Python 3.7+
        asyncio.run(main_async_example())
    except Exception as e:
        print(f"Error running asyncio example: {e}")
