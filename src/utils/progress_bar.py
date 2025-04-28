import asyncio
import logging
from typing import List, Coroutine, Any, Dict, Tuple, Optional
from rich.progress import Progress, TaskID
from rich.logging import RichHandler
from rich.console import Console

# --- ロギング設定 ---
# (他のコードと干渉しないように、標準的な設定をここに含めます)
log_console = Console(stderr=True)
logging.basicConfig(
    level="INFO", format="%(message)s", datefmt="[%X]",
    handlers=[RichHandler(console=log_console, show_path=False, markup=True)]
)
log = logging.getLogger("progress_wrapper")

# --- "最高のラッパー" 関数 ---

async def run_tasks_with_progress(
    group_definitions: Dict[str, List[Coroutine]],
    progress: Progress,
    return_exceptions: bool = False,
) -> Dict[str, List[Any]]:
    """
    複数のタスクグループを並行実行し、rich.progressでグループごとの進捗を表示する汎用ラッパー。

    Args:
        group_definitions: グループ名をキー、そのグループで実行するコルーチンオブジェクトのリストを値とする辞書。
                           例: {"Group A": [task1(), task2()], "Group B": [task3()]}
        progress: rich.progress.Progress オブジェクトのインスタンス。
        return_exceptions: asyncio.gatherに渡すパラメータ。Trueの場合、例外も結果リストに含まれる。

    Returns:
        グループ名をキー、そのグループのタスク結果（または例外）のリストを値とする辞書。
    """

    group_results: Dict[str, List[Any]] = {name: [] for name in group_definitions}
    wrapper_tasks: List[Coroutine] = []

    # --- 内部ヘルパー: 1つのグループを処理し、プログレスを更新 ---
    async def _process_single_group(
        group_name: str,
        tasks_in_group: List[Coroutine],
        group_task_id: TaskID
    ):
        num_tasks = len(tasks_in_group)
        completed_count = 0
        error_occurred = False
        group_specific_results = []

        # 開始時にプログレスバーの状態を更新
        progress.update(group_task_id, description=f"{group_name} ([yellow]処理中[/])... 0/{num_tasks}", total=num_tasks, completed=0)
        progress.start_task(group_task_id)

        # as_completed でグループ内のタスクを実行し、完了ごとに更新
        for future in asyncio.as_completed(tasks_in_group):
            try:
                result = await future
                group_specific_results.append(result)
                completed_count += 1
                # 正常完了時のプログレス更新
                progress.update(group_task_id, completed=completed_count,
                                description=f"{group_name} ([yellow]処理中[/])... {completed_count}/{num_tasks}")
            except Exception as e:
                error_occurred = True
                log.error(f"タスクグループ '{group_name}' 内でエラー: {e}", exc_info=False) # exc_info=Trueで詳細表示
                # 例外が発生した場合も結果リストに追加 (return_exceptions=Trueと同様の動作)
                group_specific_results.append(e)
                completed_count += 1 # エラーでも試行済みとしてカウント
                # エラー発生時のプログレス更新
                progress.update(group_task_id, completed=completed_count,
                                description=f"{group_name} ([red]エラー発生中[/])... {completed_count}/{num_tasks}")

        # グループ全体の処理完了後の最終状態更新
        if error_occurred:
            final_description = f"[bold red]✗ {group_name} (エラーあり)"
        else:
            final_description = f"[green]✓ {group_name} (完了)"

        progress.update(group_task_id, description=final_description)
        progress.stop_task(group_task_id) # スピナーを停止

        # このグループの結果をメインの辞書に格納
        group_results[group_name] = group_specific_results
        # このヘルパー関数自体の戻り値は使わないが、完了を示すために返す
        return f"{group_name} processed"

    # --- メインロジック ---
    # 1. 各グループに対応するプログレスバータスクを作成
    group_task_ids: Dict[str, TaskID] = {}
    for name, tasks in group_definitions.items():
        if tasks: # タスクリストが空でないグループのみ追加
            group_task_ids[name] = progress.add_task(f"{name} (待機中...)", total=len(tasks), start=False)
        else:
            log.warning(f"タスクグループ '{name}' には実行するタスクがありません。スキップします。")

    # 2. 各グループを処理するラッパーコルーチンを作成
    for name, tasks in group_definitions.items():
        if name in group_task_ids: # タスクIDが作成されたグループのみ
            task_id = group_task_ids[name]
            wrapper_tasks.append(
                _process_single_group(name, tasks, task_id)
            )

    # 3. 全てのグループ処理ラッパーを並行実行
    if wrapper_tasks:
        log.info(f"{len(wrapper_tasks)}個のタスクグループの処理を開始します...")
        # gather 自体のエラーハンドリングはここでは行わない (呼び出し元で行うか、必要なら追加)
        # return_exceptions は _process_single_group 内で模倣しているので False でも良い
        await asyncio.gather(*wrapper_tasks, return_exceptions=return_exceptions)
        log.info("全てのタスクグループの処理が完了しました。")
    else:
        log.info("実行するタスクグループがありませんでした。")

    return group_results

# --- 使用例 ---

# 1. 元の非同期処理関数 (変更不要)
async def original_task_logic(group: str, item_id: int, fail_rate: float = 0.1):
    """プログレスバーについて何も知らない、元の処理ロジック"""
    duration = 0.2 + random.random() * 0.8
    log.debug(f"[{group}-{item_id}] 実行開始 ({duration:.2f}s)")
    await asyncio.sleep(duration)
    if random.random() < fail_rate:
        raise ValueError(f"'{group}-{item_id}'で意図的なエラー発生")
    log.debug(f"[{group}-{item_id}] 実行完了")
    return f"{group}-{item_id} Result"

# 2. メインの実行部分
async def main():
    from rich.progress import (SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn,
                               TimeElapsedColumn, TimeRemainingColumn)

    # プログレスバーのカラム設定
    progress_columns = [
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), MofNCompleteColumn(), TimeElapsedColumn(), TimeRemainingColumn()
    ]

    # --- 元のコードを準備 ---
    # グループ名と、そのグループで実行したい元のコルーチンのリストを定義
    groups: Dict[str, List[Coroutine]] = {
        "データロード": [original_task_logic("Load", i, 0.05) for i in range(8)],
        "前処理": [original_task_logic("Preproc", i, 0.1) for i in range(12)],
        "モデル予測": [original_task_logic("Predict", i, 0.02) for i in range(5)],
        "後処理": [original_task_logic("Postproc", i, 0.15) for i in range(10)],
        "空のグループ": [], # 空のグループのテスト
    }

    # --- ラッパー関数を呼び出す ---
    # Progressオブジェクトを作成してラッパーに渡す
    async with Progress(*progress_columns, transient=False) as progress:
        all_results = await run_tasks_with_progress(groups, progress)

    # --- 結果の確認 ---
    log.info("--- 実行結果 ---")
    for group_name, results in all_results.items():
        success_count = sum(1 for r in results if not isinstance(r, Exception))
        error_count = len(results) - success_count
        log.info(f"グループ '{group_name}': 成功 {success_count}件, 失敗 {error_count}件")
        # 必要なら個別の結果やエラーを表示
        # for i, res in enumerate(results):
        #     if isinstance(res, Exception):
        #         log.warning(f"  - Task {i} failed: {res}")
        #     else:
        #         log.info(f"  - Task {i} success: {res}")


if __name__ == "__main__":
    # ログレベルをDEBUGに変更して詳細を確認する場合
    # logging.getLogger("progress_wrapper").setLevel(logging.DEBUG)
    asyncio.run(main())