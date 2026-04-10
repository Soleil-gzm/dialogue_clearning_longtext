#!/usr/bin/env python3
"""
多轮对话拆分脚本
将原始 JSON 文件中的每个对话按轮次拆分成样本，保存为 JSONL 文件，并统计轮次分布。
支持流式读取、分批输出、断点续传。
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path
import ijson
from tqdm import tqdm

# ========== 配置 ==========
INPUT_JSON = "data/data-record-processed-92049-filter.json"   # 原始 JSON 文件路径
OUTPUT_DIR = "samples"                        # 输出目录
STATS_DIR = "stats"                           # 统计轮次分布文件目录
PROGRESS_FILE = "progress.txt"                # 进度文件
BATCH_SIZE = 120000                             # 每批处理的对话数量（每个 JSONL 文件包含的对话数）

# ========== 辅助函数 ==========
def get_last_processed_index(progress_file):
    """读取上次处理到的对话索引"""
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return -1
    return -1

def update_progress(progress_file, index):
    """更新进度文件 将当前处理到的对话索引写入进度文件，用于断点续传。"""
    with open(progress_file, 'w') as f:
        f.write(str(index))

def get_output_filename(batch_start, batch_end):
    """根据对话索引范围生成输出文件名"""
    return f"sample_{batch_start:08d}_{batch_end:08d}.jsonl"

def process_dialog(dialog_id, messages, turn_counter):
    """
    处理单个对话，负责将一个对话拆分成多个样本。,生成样本列表（包含完整历史)
    假设 messages[0] 是 system，messages[1] 是空 user（视为第一轮用户输入），
    后面 user 和 assistant 交替出现。
    输出样本中：
        user_input: 以 "Q：" 开头，后跟历史对话内容（无前缀）和当前用户输入（无前缀）
        target_output: 以 "A：" 开头，后跟当前助手输出（无前缀）

    samples：存储当前对话拆分出的所有样本（每个样本对应一轮）。
    history_pairs：列表，元素为 (user_raw, assistant_raw) 的元组，保存之前所有轮的原始文本（无前缀），用于构建下一轮的历史上下文。
    pending_user：暂存最近遇到的 user 消息，等待与之配对的 assistant 消息。如果遇到连续 user 或异常情况，会被覆盖或忽略，但代码假定数据格式正确（user 和 assistant 交替出现）。
    turn：当前轮次编号，从 0 开始，等于 len(samples)（即已经产生的样本数）。
    """
    samples = []
    history_pairs = []   
    pending_user = None
    # 从索引1开始，跳过 system
    for i, msg in enumerate(messages):
        role = msg.get('role')
        content = msg.get('content', '')
        if role == 'user':
            pending_user = msg
        elif role == 'assistant' and pending_user is not None:
            # 组成一轮
            turn = len(samples)
            user_raw = pending_user.get('content', '')
            assistant_raw = msg.get('content', '')

            # 构建历史文本（原始内容，不加前缀）
            history_text = ""
            for hist_user_raw, hist_assistant_raw in history_pairs:
                history_text += f"{hist_user_raw}\n{hist_assistant_raw}\n"

            # 当前用户输入（原始）
            current_input = user_raw
            # user_input 以 "Q：" 开头，后接历史+当前用户输入
            if history_text:
                full_input = f"Q：{history_text}{current_input}"
            else:
                full_input = f"Q：{current_input}" if current_input else "Q："

            # target_output 以 "A：" 开头
            target_output = f"A：{assistant_raw}" if assistant_raw else "A："

            sample = {
                "id": dialog_id,
                "turn": turn,
                "user_input": full_input,
                "target_output": target_output,
                "loss": msg.get('loss', False),
                # 可选：添加 "text" 字段用于清洗（仅当前轮对话，不带前缀）
                # "text": f"{user_raw}\n{assistant_raw}"  # 可保留用于某些清洗算子
            }
            samples.append(sample)
            turn_counter[turn] += 1
            # 将当前轮加入历史（存储原始文本，不加前缀）
            history_pairs.append((user_raw, assistant_raw))
            pending_user = None
    return samples

def main():
    # 创建输出目录
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(STATS_DIR).mkdir(parents=True, exist_ok=True)

    # 读取进度
    last_idx = get_last_processed_index(PROGRESS_FILE)
    start_idx = last_idx + 1
    if start_idx <= 0:
        print("从头开始处理...")
    else:
        print(f"从第 {start_idx} 条对话继续处理...")

    # 检查输入文件是否存在
    if not os.path.exists(INPUT_JSON):
        print(f"错误：输入文件 {INPUT_JSON} 不存在")
        sys.exit(1)

    # 统计计数器
    turn_counter = defaultdict(int)   # 统计各轮次样本数

    # 批次控制
    batch_start = start_idx         # 当前批次第一个对话的索引
    batch_end = start_idx - 1       # 初始值，后面会更新
    current_file = None             # 当前打开的输出文件对象
    current_file_path = None        # 当前文件路径
    current_file_count = 0          # 当前文件已写入的对话数（注意是对话数，不是样本数）

    # 流式读取 JSON 数组
    with open(INPUT_JSON, 'rb') as f:
        # 使用 ijson 迭代每个对话对象
        items = ijson.items(f, 'item')
        # 为了显示进度，我们先用 tqdm 包装，但无法提前知道总数，可以手动更新
        # 这里使用 tqdm 的 manual 模式，通过更新已处理数量
        # 进度条，每处理一个对话更新一次。
        pbar = tqdm(desc="Processing dialogues", unit="dialogue")
        processed = 0
        current_idx = 0
        for dialog in items:
            # 跳过已经处理过的
            if current_idx < start_idx:
                current_idx += 1
                continue

            dialog_id = current_idx  # 使用数组索引作为 ID
            messages = dialog.get('messages', [])
            if not messages:
                current_idx += 1
                processed += 1
                pbar.update(1)
                continue

            # 处理当前对话
            samples = process_dialog(dialog_id, messages, turn_counter)

            # 写入当前批次文件
            # 检查是否需要开始新批次
            if current_file is None or current_file_count >= BATCH_SIZE:
                if current_file:
                    current_file.close()
                # 计算批次结束索引
                batch_start = current_idx
                batch_end = batch_start + BATCH_SIZE - 1
                output_filename = get_output_filename(batch_start, batch_end)
                current_file_path = os.path.join(OUTPUT_DIR, output_filename)
                current_file = open(current_file_path, 'a', encoding='utf-8')
                current_file_count = 0
                print(f"创建新批次文件: {output_filename}")

            # 写入样本
            for sample in samples:
                current_file.write(json.dumps(sample, ensure_ascii=False) + '\n')
                current_file_count += 1

            # 更新进度
            current_idx += 1
            processed += 1
            if processed % 1000 == 0:
                update_progress(PROGRESS_FILE, current_idx - 1)   # 记录已处理的最大索引
            pbar.update(1)

        # 关闭最后一个文件
        if current_file:
            current_file.close()
        pbar.close()

    # 最终更新进度
    update_progress(PROGRESS_FILE, current_idx - 1)

    # 保存统计结果
    stats = {
        "total_samples": sum(turn_counter.values()),
        "turn_distribution": dict(turn_counter)
    }
    stats_path = os.path.join(STATS_DIR, "turn_distribution.json")
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"统计结果已保存到 {stats_path}")

    # 打印统计摘要
    print("\n轮次分布摘要：")
    for turn, cnt in sorted(turn_counter.items()):
        print(f"  第 {turn} 轮: {cnt} 条样本")
    print(f"总样本数: {stats['total_samples']}")

if __name__ == "__main__":
    main()