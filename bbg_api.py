import blpapi
import time
import logging
from functools import wraps

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def retry_bloomberg_request(max_retries=3, backoff_factor=2, session_restart=True):
    """Bloombergリクエストを指定回数リトライするデコレータ"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_session = None
            
            # セッション情報が引数に含まれていれば取得
            for arg in args:
                if isinstance(arg, blpapi.Session):
                    current_session = arg
                    break
            
            if current_session is None and 'session' in kwargs:
                current_session = kwargs['session']
            
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    wait_time = backoff_factor ** retries
                    error_msg = str(e)
                    
                    # エラー種別の判定
                    if "Request limit exceeded" in error_msg:
                        logger.warning(f"Bloomberg API接続制限エラー: {e}. {wait_time}秒後にリトライします。({retries}/{max_retries})")
                    elif "Invalid security" in error_msg or "Invalid field" in error_msg:
                        logger.error(f"データ不在エラー: {e}")
                        return None  # データが存在しない場合は終了
                    else:
                        logger.warning(f"Bloomberg APIエラー: {e}. {wait_time}秒後にリトライします。({retries}/{max_retries})")
                    
                    if retries == max_retries:
                        logger.error(f"最大リトライ回数に達しました: {e}")
                        raise
                    
                    # セッション再起動が必要な場合
                    if session_restart and current_session:
                        try:
                            logger.info("セッションを再起動します")
                            current_session.stop()
                            time.sleep(1)
                            current_session.start()
                        except Exception as session_error:
                            logger.error(f"セッション再起動エラー: {session_error}")
                    
                    time.sleep(wait_time)
            
            return None
        return wrapper
    return decorator

# 使用例
@retry_bloomberg_request(max_retries=3, backoff_factor=2)
def get_bloomberg_data(session, securities, fields, options=None):
    """Bloombergからデータを取得する関数"""
    # 実際のデータ取得処理
    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")
    
    # 証券情報を追加
    for security in securities:
        request.append("securities", security)
    
    # フィールド情報を追加
    for field in fields:
        request.append("fields", field)
    
    # オプションがあれば追加
    if options:
        for key, value in options.items():
            request.set(key, value)
    
    # リクエスト送信
    session.sendRequest(request)
    
    # レスポンス待機と処理
    response_data = {}
    done = False
    timeout = 30  # タイムアウト秒数
    start_time = time.time()
    
    while not done and (time.time() - start_time) < timeout:
        event = session.nextEvent(500)  # 500msごとにイベントチェック
        
        if event.eventType() == blpapi.Event.RESPONSE:
            for msg in event:
                # レスポンス処理ロジック（具体的な実装はデータ形式による）
                # ...
            done = True
        elif event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
            for msg in event:
                # 部分レスポンス処理
                # ...
        elif event.eventType() == blpapi.Event.TIMEOUT:
            logger.warning("タイムアウトが発生しました")
            return None
    
    if not done:
        logger.error("応答待ちタイムアウト")
        return None
        
    return response_data

import pickle
import os
import time
from functools import wraps

class BloombergDataCache:
    def __init__(self, cache_dir='./bloomberg_cache', expiry_days=7):
        self.cache_dir = cache_dir
        self.expiry_seconds = expiry_days * 24 * 60 * 60
        
        # キャッシュディレクトリが存在しない場合は作成
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        # データ不在情報を保存するための特別なキャッシュファイル
        self.nonexistent_data_file = os.path.join(cache_dir, 'nonexistent_data.pkl')
        self.nonexistent_data = self._load_nonexistent_data()
    
    def _load_nonexistent_data(self):
        """データ不在情報をロード"""
        if os.path.exists(self.nonexistent_data_file):
            try:
                with open(self.nonexistent_data_file, 'rb') as f:
                    return pickle.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_nonexistent_data(self):
        """データ不在情報を保存"""
        with open(self.nonexistent_data_file, 'wb') as f:
            pickle.dump(self.nonexistent_data, f)
    
    def _generate_cache_key(self, securities, fields, options=None):
        """キャッシュキーを生成"""
        key_parts = [str(securities), str(fields)]
        if options:
            key_parts.append(str(options))
        
        # キー文字列から有効なファイル名を生成
        import hashlib
        return hashlib.md5('_'.join(key_parts).encode()).hexdigest()
    
    def get(self, securities, fields, options=None):
        """キャッシュからデータを取得"""
        cache_key = self._generate_cache_key(securities, fields, options)
        
        # データ不在チェック
        if cache_key in self.nonexistent_data:
            timestamp = self.nonexistent_data[cache_key]
            if time.time() - timestamp < self.expiry_seconds:
                # 期限内のデータ不在情報
                return None, True  # (None, is_nonexistent_data)
        
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
        
        if os.path.exists(cache_file):
            # ファイルの更新時間をチェック
            modified_time = os.path.getmtime(cache_file)
            if time.time() - modified_time < self.expiry_seconds:
                try:
                    with open(cache_file, 'rb') as f:
                        return pickle.load(f), False
                except Exception:
                    # 読み込みエラーなら無視してAPIリクエストへ
                    pass
        
        return None, False
    
    def set(self, securities, fields, data, options=None):
        """データをキャッシュに保存"""
        cache_key = self._generate_cache_key(securities, fields, options)
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.pkl")
        
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
    
    def set_nonexistent(self, securities, fields, options=None):
        """データが存在しないことをキャッシュ"""
        cache_key = self._generate_cache_key(securities, fields, options)
        self.nonexistent_data[cache_key] = time.time()
        self._save_nonexistent_data()
    
    def clear_expired(self):
        """期限切れのキャッシュを削除"""
        # 通常のデータキャッシュのクリーンアップ
        for filename in os.listdir(self.cache_dir):
            if filename.endswith('.pkl') and filename != 'nonexistent_data.pkl':
                filepath = os.path.join(self.cache_dir, filename)
                if time.time() - os.path.getmtime(filepath) > self.expiry_seconds:
                    os.remove(filepath)
        
        # データ不在情報のクリーンアップ
        expired_keys = []
        for key, timestamp in self.nonexistent_data.items():
            if time.time() - timestamp > self.expiry_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.nonexistent_data[key]
        
        if expired_keys:
            self._save_nonexistent_data()

# キャッシュを使用したBloombergデータ取得関数の例
def get_bloomberg_data_with_cache(session, securities, fields, options=None, cache=None):
    """キャッシュを活用してBloombergからデータを取得する"""
    if cache:
        cached_data, is_nonexistent = cache.get(securities, fields, options)
        if is_nonexistent:
            logger.info(f"キャッシュ: データ不在情報が見つかりました")
            return None
        if cached_data is not None:
            logger.info(f"キャッシュ: データを返却します")
            return cached_data
    
    try:
        # 実際のBloombergリクエスト処理
        data = get_bloomberg_data(session, securities, fields, options)
        
        if data is None:
            # データが存在しないことをキャッシュ
            if cache:
                logger.info(f"キャッシュ: データ不在情報を保存します")
                cache.set_nonexistent(securities, fields, options)
        else:
            # 正常なデータをキャッシュ
            if cache:
                logger.info(f"キャッシュ: データを保存します")
                cache.set(securities, fields, data, options)
        
        return data
    except Exception as e:
        # 接続エラーやその他のAPIエラーの場合はキャッシュせずに例外を再送出
        logger.error(f"Bloomberg APIエラー: {e}")
        raise

### 使用例
# キャッシュ初期化
cache = BloombergDataCache(cache_dir='./bloomberg_cache', expiry_days=7)

# 週次でキャッシュのクリーンアップを実行（例：スケジュールタスクで）
cache.clear_expired()

# データ取得時の使用例
try:
    # 証券IDとフィールドのリスト
    securities = ["AAPL US Equity", "MSFT US Equity"]
    fields = ["PX_LAST", "VOLUME", "PE_RATIO"]
    options = {"startDate": "20230101", "endDate": "20230131"}
    
    # キャッシュを使ったデータ取得
    data = get_bloomberg_data_with_cache(session, securities, fields, options, cache)
    
    if data is None:
        print("データが存在しないか取得できませんでした")
    else:
        print("データを取得しました:", len(data), "件")
except Exception as e:
    print(f"エラーが発生しました: {e}")
