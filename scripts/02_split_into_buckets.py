#!/usr/bin/env python3
"""
split_into_buckets.py
将 samples/ 目录下的所有 JSONL 文件按 turn 值分桶，输出到 bucketed/ 目录
桶定义：
   bucket_0_2  : turn 0,1,2
   bucket_3_5  : turn 3,4,5
   bucket_6_10 : turn 6,7,8,9,10
   bucket_11_20: turn 11..20
   bucket_21plus: turn >=21
"""

import json
import os
from pathlib import Path
from collections import defaultdict

# 配置
INPUT_DIR = "samples"
OUTPUT_BASE = "bucketed"

BUCKETS = {
    (0, 0): "bucket_0",
    (1, 1):"bucket_1",
    (2, 2): "bucket_2",
    (3, 3): "bucket_3",
    (4, 4): "bucket_4",
    (5, 5): "bucket_5",
    (6, 6): "bucket_6",
    (7, 7): "bucket_7",
    (8, 8): "bucket_8",
    (9, 9): "bucket_9",
    (10, 10): "bucket_10",
    (11, 11): "bucket_11",
    (12, 12):"bucket_12",
    (13, 22): "bucket_13_22",
    (23, float('inf')): "bucket_23plus",
}

'''
根据 turn 值查找对应的桶名称。
遍历 BUCKETS 的每一项，如果 low <= turn <= high 则返回该桶名。
如果没找到（理论上不会发生，因为最后一个桶覆盖到无穷大），则返回 "bucket_23plus" 作为备用名称。
'''
def get_bucket_name(turn):
    for (low, high), name in BUCKETS.items():
        if low <= turn <= high:
            return name
    return "bucket_23plus"  # fallback

def main():
    input_path = Path(INPUT_DIR)
    if not input_path.exists():
        print(f"错误：目录 {INPUT_DIR} 不存在，请先运行拆分脚本生成 samples/")
        return

    output_base = Path(OUTPUT_BASE)
    output_base.mkdir(parents=True, exist_ok=True)

    # 为每个桶创建子目录并清空（避免旧数据干扰）
    bucket_dirs = {}
    for name in set(get_bucket_name(i) for i in range(0, 100)):
        bucket_dir = output_base / name
        bucket_dir.mkdir(exist_ok=True)
        # 清空目录内容（可选，注释掉则不清空）
        for f in bucket_dir.glob("*.jsonl"):
            f.unlink()
        bucket_dirs[name] = bucket_dir

    # 遍历所有 JSONL 文件
    jsonl_files = list(input_path.glob("*.jsonl"))
    print(f"找到 {len(jsonl_files)} 个 JSONL 文件")

    # 为每个桶准备写入文件（保持原文件名，但放入对应桶目录）
    # 由于一个文件内可能包含多种 turn，我们需要为每个桶动态打开文件
    # 简单起见：对每个输入文件，遍历其行，按桶写入多个输出文件（同名）
    for input_file in jsonl_files:
        print(f"处理: {input_file.name}")
        # 为每个桶准备该文件的输出句柄（延迟打开）
        file_handles = {}
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    turn = data.get('turn')
                    if turn is None:
                        continue
                    bucket = get_bucket_name(turn)
                    # 获取输出文件路径
                    output_file = bucket_dirs[bucket] / input_file.name
                    # 打开文件句柄（追加模式）
                    if output_file not in file_handles:
                        file_handles[output_file] = open(output_file, 'a', encoding='utf-8')
                    file_handles[output_file].write(line)
        finally:
            for h in file_handles.values():
                h.close()

    # 打印统计
    print("\n分桶完成，各桶文件统计：")
    for name, dir_path in bucket_dirs.items():
        count = sum(1 for _ in dir_path.glob("*.jsonl"))
        print(f"  {name}: {count} 个文件")

if __name__ == "__main__":
    main()