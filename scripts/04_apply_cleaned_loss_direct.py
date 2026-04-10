#!/usr/bin/env python3
"""
直接从清洗后的 JSONL 文件中收集保留的 (id, turn) 对，
应用到原始 JSON 上，输出最终训练数据 JSON。

特性：
- 自动选择最新时间戳或手动指定
- 支持未清洗桶：可配置为“全部保留”或“全部丢弃”
- 详细的统计信息
- 输出文件命名清晰
- 保留原始 JSON 结构，只修改 loss 字段
"""

import json
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# ========== 默认配置 ==========
DEFAULT_ORIGINAL_JSON = "data/data-record-processed-92049-filter.json"
DEFAULT_CLEANED_ROOT = "cleaned_jsonl"
DEFAULT_OUTPUT_DIR = "final_training_data"
# 未清洗桶：如果某个桶没有清洗结果，但你想全部保留其对应的轮次，在此列出桶名
# 注意：需要知道每个桶对应的 turn 范围，才能为每个对话生成正确的 turn 集合。
# 简化版：对于未清洗桶，我们假设其覆盖的 turn 范围是已知的，然后为所有对话中落在该范围内的 turn 保留。
# 但通常更简单的是：确保所有桶都清洗过（方案A），不依赖此功能。
# 这里提供一个可选的处理方式：如果未清洗桶列表非空，会尝试为每个对话生成所有 assistant turn 的集合（即全部保留），
# 然后与清洗桶的保留集合合并。但这可能导致未清洗桶覆盖的 turn 范围外的 turn 也被错误保留。
# 更严谨的做法是要求用户明确每个桶的 turn 范围映射。为了生产可用，推荐方案A（所有桶都清洗）。
UNWASHED_BUCKETS = []   # 例如 ["bucket_41plus"]，但需要配合 turn 范围映射

# 如果你真的需要处理未清洗桶，请定义每个桶的 turn 范围（包含）
BUCKET_TURN_RANGE = {
    # "bucket_turn0": (0, 0),
    # "bucket_turn1": (1, 1),
    # "bucket_3_5": (3, 5),
    # "bucket_6_10": (6, 10),
    # "bucket_11_22": (11, 22),
    # "bucket_23plus": (23, float('inf')),
}


def get_latest_timestamp(cleaned_root):
    """获取 cleaned_root 下最新的时间戳目录名"""
    cleaned_dir = Path(cleaned_root)
    if not cleaned_dir.exists():
        return None
    timestamps = [d.name for d in cleaned_dir.iterdir() if d.is_dir()]
    if not timestamps:
        return None
    timestamps.sort(reverse=True)
    return timestamps[0]


def collect_kept_turns_from_cleaned(cleaned_timestamp_dir, unwashed_buckets=None):
    """
    从清洗结果目录中收集所有保留的 (id, turn)
    cleaned_timestamp_dir: Path 对象，如 cleaned_jsonl/20250402_133517/
    unwashed_buckets: 未清洗桶列表（这些桶的目录不会被扫描，稍后单独处理）
    返回: kept_turns = {dialog_id: set(turns)}
    """
    kept = defaultdict(set)
    # 遍历所有桶子目录
    for bucket_dir in cleaned_timestamp_dir.iterdir():
        if not bucket_dir.is_dir():
            continue
        bucket_name = bucket_dir.name
        if unwashed_buckets and bucket_name in unwashed_buckets:
            print(f"  跳过未清洗桶目录: {bucket_name}")
            continue
        # 扫描该桶下的所有 JSONL 文件
        jsonl_files = list(bucket_dir.glob("*.jsonl"))
        if not jsonl_files:
            continue
        for jsonl_file in jsonl_files:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        dialog_id = data.get('id')
                        turn = data.get('turn')
                        if dialog_id is not None and turn is not None:
                            kept[dialog_id].add(turn)
                    except json.JSONDecodeError as e:
                        print(f"警告: {jsonl_file} 第 {line_num} 行解析失败: {e}")
    return kept


def add_unwashed_buckets_turns(original_dialogues, kept_turns, unwashed_buckets, bucket_turn_range):
    """
    为未清洗桶添加所有应该保留的 turn。
    这里采用保守策略：如果某个桶被标记为未清洗，且定义了其 turn 范围，
    则对于每个对话，将该范围内存在的所有 assistant turn 加入 kept_turns。
    注意：此操作会修改 kept_turns 字典。
    """
    if not unwashed_buckets:
        return kept_turns

    # 构建一个快速查找：哪些桶需要处理，以及它们的 turn 范围
    bucket_ranges = {name: rng for name, rng in bucket_turn_range.items() if name in unwashed_buckets}
    if not bucket_ranges:
        print("警告: 未清洗桶列表非空，但未提供对应的 turn 范围，将不处理任何未清洗桶。")
        return kept_turns

    # 遍历每个对话，找出所有 assistant 消息的 turn
    # 注意：这里假设对话中 assistant 消息的顺序即为 turn 编号（从0开始）
    for dialog_id, dialog in enumerate(original_dialogues):
        messages = dialog.get('messages', [])
        assistant_turn = 0
        for msg in messages:
            if msg.get('role') == 'assistant':
                # 检查这个 turn 是否属于任何一个未清洗桶的 turn 范围
                for bucket_name, (low, high) in bucket_ranges.items():
                    if low <= assistant_turn <= high:
                        kept_turns[dialog_id].add(assistant_turn)
                        break
                assistant_turn += 1
    return kept_turns


def apply_loss_to_original(original_dialogues, kept_turns):
    """
    根据 kept_turns 修改原始对话中的 loss 字段。
    返回新的对话列表（不修改原始对象，但为了性能直接修改并返回原列表也可，这里返回新列表）。
    """
    restored = []
    total_assistant = 0
    total_true = 0
    for dialog_id, dialog in enumerate(original_dialogues):
        messages = dialog.get('messages', [])
        # 第一遍：将所有 assistant 的 loss 设为 False，并记录其 turn 索引
        assistant_indices = []
        for idx, msg in enumerate(messages):
            if msg.get('role') == 'assistant':
                msg['loss'] = "False"
                assistant_indices.append(idx)
                total_assistant += 1
        # 第二遍：根据 kept_turns 将对应的 loss 设为 True
        for turn in kept_turns.get(dialog_id, set()):
            if turn < len(assistant_indices):
                msg_idx = assistant_indices[turn]
                messages[msg_idx]['loss'] = "True"
                total_true += 1
            else:
                print(f"警告: 对话 {dialog_id} 中 turn {turn} 超出范围 (共 {len(assistant_indices)} 个 assistant)")
        restored.append(dialog)
    print(f"统计: 总 assistant 消息数 = {total_assistant}, 保留(True) = {total_true}, 丢弃(False) = {total_assistant - total_true}")
    return restored


def main():
    parser = argparse.ArgumentParser(description="直接应用清洗结果生成最终训练 JSON")
    parser.add_argument("--original", type=str, default=DEFAULT_ORIGINAL_JSON,
                        help=f"原始 JSON 文件路径 (默认: {DEFAULT_ORIGINAL_JSON})")
    parser.add_argument("--cleaned_root", type=str, default=DEFAULT_CLEANED_ROOT,
                        help=f"清洗结果根目录 (默认: {DEFAULT_CLEANED_ROOT})")
    parser.add_argument("--output_dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"输出目录 (默认: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--timestamp", type=str, default=None,
                        help="指定时间戳 (默认: 自动选择最新)")
    parser.add_argument("--unwashed_buckets", type=str, nargs='+', default=UNWASHED_BUCKETS,
                        help="未清洗桶名称列表，这些桶内的样本将全部保留 (需配合 --bucket_turn_range)")
    parser.add_argument("--bucket_turn_range", type=str, nargs='+', default=[],
                        help="桶的 turn 范围，格式: bucket_name low high，例如 bucket_23plus 23 inf")
    args = parser.parse_args()

    # 解析桶范围
    bucket_turn_range = {}
    for item in args.bucket_turn_range:
        parts = item.split()
        if len(parts) != 3:
            print(f"警告: 忽略无效的桶范围格式: {item}")
            continue
        name, low, high = parts
        low = int(low)
        high = float('inf') if high.lower() == 'inf' else int(high)
        bucket_turn_range[name] = (low, high)

    print("="*60)
    print("直接应用清洗结果生成最终训练数据")
    print("="*60)

    # 确定时间戳
    cleaned_root_path = Path(args.cleaned_root)
    if args.timestamp:
        timestamp = args.timestamp
        cleaned_timestamp_dir = cleaned_root_path / timestamp
        if not cleaned_timestamp_dir.exists():
            print(f"错误: 指定的时间戳目录不存在: {cleaned_timestamp_dir}")
            return
    else:
        timestamp = get_latest_timestamp(cleaned_root_path)
        if timestamp is None:
            print("错误: 未找到清洗结果目录，请先运行清洗脚本")
            return
        cleaned_timestamp_dir = cleaned_root_path / timestamp
    print(f"使用时间戳: {timestamp}")
    print(f"清洗结果目录: {cleaned_timestamp_dir}")

    # 加载原始 JSON
    original_path = Path(args.original)
    if not original_path.exists():
        print(f"错误: 原始 JSON 文件不存在: {original_path}")
        return
    print(f"加载原始对话: {original_path}")
    with open(original_path, 'r', encoding='utf-8') as f:
        original_dialogues = json.load(f)
    print(f"原始对话数量: {len(original_dialogues)}")

    # 收集清洗桶保留的样本
    print("扫描清洗结果，收集保留的 (id, turn)...")
    kept_turns = collect_kept_turns_from_cleaned(cleaned_timestamp_dir, args.unwashed_buckets)
    print(f"从清洗桶中收集到 {len(kept_turns)} 个对话，总保留轮次: {sum(len(v) for v in kept_turns.values())}")

    # 处理未清洗桶（如果提供）
    if args.unwashed_buckets:
        print(f"处理未清洗桶: {args.unwashed_buckets}")
        kept_turns = add_unwashed_buckets_turns(original_dialogues, kept_turns,
                                                args.unwashed_buckets, bucket_turn_range)
        print(f"加入未清洗桶后，总保留轮次: {sum(len(v) for v in kept_turns.values())}")

    # 应用 loss 标记
    print("应用 loss 标记到原始对话...")
    final_data = apply_loss_to_original(original_dialogues, kept_turns)

    # 保存最终结果
    output_root = Path(args.output_dir)
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"training_data_{timestamp}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 完成！输出文件: {output_file}")
    print(f"文件大小: {output_file.stat().st_size / (1024*1024):.2f} MB")


if __name__ == "__main__":
    main()