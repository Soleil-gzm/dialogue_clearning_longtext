#!/usr/bin/env python3
"""
分桶清洗脚本（带统计和可视化）
对每个桶的 JSONL 文件调用 Data-Juicer 清洗，并统计清洗前后的样本数量。
生成清洗报告：各桶清洗率、总体清洗率、各轮次分布对比，并绘制对比柱状图。
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# 尝试导入绘图库
try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')  # 使用非交互式后端，避免显示窗口
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("警告: matplotlib 未安装，将无法生成图表。可使用 'pip install matplotlib' 安装。")

# ========== 配置 ==========
BUCKETED_ROOT = "bucketed"
CLEANED_ROOT = "cleaned_jsonl"
TRACE_ROOT = "trace_output"
CONFIGS_DIR = "configs"
REPORT_DIR = "cleaning_reports"
# 配置：全局图要绘制的轮次，None 表示全部，否则为列表如 [0,1,2,3,4,5,6,7,8,9,10]
# PLOT_TURNS = None   #全局
PLOT_TURNS = list(range(23))

# BUCKET_CONFIG_MAP = {
#     # "bucket_turn0": "config_bucket_turn0.yaml",
#     # "bucket_turn1": "config_bucket_turn1.yaml",
#     "bucket_turn2": "config_bucket_turn2.yaml",
#     # "bucket_3_5": "config_bucket_3_5.yaml",
#     # "bucket_6_10": "config_bucket_6_10.yaml",
#     # "bucket_11_22": "config_bucket_11_22.yaml",
#     # "bucket_23plus": "config_bucket_23plus.yaml",
# }



BUCKET_CONFIG_MAP = {
    "bucket_0": "config_bucket_turn0.yaml",
    "bucket_1": "config_bucket_turn1.yaml",
    "bucket_2": "config_bucket_turn2.yaml",
    "bucket_3": "config_bucket_3.yaml",
    "bucket_4": "config_bucket_4.yaml",
    "bucket_5": "config_bucket_5.yaml",
    "bucket_6": "overal_config.yaml",
    "bucket_7": "overal_config.yaml",
    "bucket_8": "overal_config.yaml",
    "bucket_9": "overal_config.yaml",
    "bucket_10": "overal_config.yaml",
    "bucket_11": "overal_config.yaml",
    "bucket_12": "overal_config.yaml",
    "bucket_13_22": "config_bucket_13_22.yaml",
    "bucket_23plus": "config_bucket_23plus.yaml",

}


# 返回当前时间的字符串格式，用于创建带时间戳的子目录，避免覆盖之前的结果。
def get_timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def count_samples_in_jsonl(file_path):
    """ 统计 JSONL 文件的行数（每个样本一行），用于计算清洗前后的样本数量。"""
    if not file_path.exists():
        return 0
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def collect_turn_distribution(file_path):
    """ 统计文件中各 turn 的样本数量 """
    dist = defaultdict(int)
    if not file_path.exists():
        return dist
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                turn = data.get('turn')
                if turn is not None:
                    dist[turn] += 1
            except:
                pass
    return dist

def plot_turn_distribution(bucket_name, input_dist, output_dist, output_dir, selected_turns=None):
    """绘制清洗前后 turn 分布对比柱状图，可指定只绘制部分轮次
    新增参数：selected_turns=None，默认值为 None，表示绘制所有轮次。
    轮次选择逻辑：
    若 selected_turns 不为 None，则按传入的列表排序后作为横坐标。
    否则，取输入分布和输出分布的键的并集。
    中文字体设置：添加两行 rcParams，确保图表中的中文（如“轮次”、“样本数量”、“清洗前/后”）能正常显示。
    """
    if not HAS_MATPLOTLIB:
        return
    # 设置中文字体（解决中文显示问题）
    plt.rcParams['font.sans-serif'] = ['SimHei', 'WenQuanYi Zen Hei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    if not input_dist and not output_dist:
        return
    # 确定要绘制的轮次：如果指定了 selected_turns，则使用它；否则取并集
    if selected_turns is not None:
        all_turns = sorted(selected_turns)
    else:
        all_turns = sorted(set(input_dist.keys()) | set(output_dist.keys()))
    if not all_turns:
        return

    input_counts = [input_dist.get(t, 0) for t in all_turns]
    output_counts = [output_dist.get(t, 0) for t in all_turns]

    plt.figure(figsize=(12, 6))
    x = range(len(all_turns))
    width = 0.35
    plt.bar(x, input_counts, width, label='清洗前', color='steelblue')
    plt.bar([i + width for i in x], output_counts, width, label='清洗后', color='salmon')
    plt.xlabel('轮次 (turn)')
    plt.ylabel('样本数量')
    plt.title(f'{bucket_name} 清洗前后轮次分布对比')
    plt.xticks([i + width/2 for i in x], all_turns, rotation=45)
    plt.legend()
    plt.tight_layout()
    plot_path = output_dir / f'{bucket_name}_turn_distribution.png'
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"    图表已保存: {plot_path}")

def plot_overall_distribution(overall_input_dist, overall_output_dist, output_dir, timestamp):
    """绘制全局的清洗前后 turn 分布对比柱状图"""
    if not HAS_MATPLOTLIB:
        return
    if not overall_input_dist and not overall_output_dist:
        return
    all_turns = sorted(set(overall_input_dist.keys()) | set(overall_output_dist.keys()))
    if not all_turns:
        return
    input_counts = [overall_input_dist.get(t, 0) for t in all_turns]
    output_counts = [overall_output_dist.get(t, 0) for t in all_turns]

    plt.figure(figsize=(14, 6))
    x = range(len(all_turns))
    width = 0.35
    plt.bar(x, input_counts, width, label='清洗前', color='steelblue')
    plt.bar([i + width for i in x], output_counts, width, label='清洗后', color='salmon')
    plt.xlabel('轮次 (turn)')
    plt.ylabel('样本数量')
    plt.title(f'全局清洗前后轮次分布对比 (时间戳: {timestamp})')
    plt.xticks([i + width/2 for i in x], all_turns, rotation=45)
    plt.legend()
    plt.tight_layout()
    plot_path = output_dir / f'overall_turn_distribution.png'
    plt.savefig(plot_path, dpi=150)
    plt.close()
    print(f"全局图表已保存: {plot_path}")

def clean_bucket(bucket_dir, config_file, output_dir, trace_dir, stats):
    """
    清洗一个桶内的所有 JSONL 文件，并收集统计信息。
    stats 是一个字典，用于存储该桶的总体统计。
    """
    if not bucket_dir.exists():
        print(f"  目录不存在: {bucket_dir}")
        return 0

    input_files = list(bucket_dir.glob("*.jsonl"))
    if not input_files:
        print(f"  没有找到 JSONL 文件")
        return 0

    print(f"\n  发现 {len(input_files)} 个文件")

    output_dir.mkdir(parents=True, exist_ok=True)
    trace_dir.mkdir(parents=True, exist_ok=True)

    # 用于累加该桶内所有文件的样本总数和 turn 分布。
    bucket_stats = {
        "input_samples": 0,
        "output_samples": 0,
        "input_turn_dist": defaultdict(int),
        "output_turn_dist": defaultdict(int),
    }

    success_count = 0       # 记录成功清洗的文件数量
    for input_file in input_files:
        output_file = output_dir / input_file.name      # 清洗后的输出文件路径，保持原文件名
        trace_subdir = trace_dir / input_file.stem      # 每个文件单独的 trace 子目录

        # 清洗前统计
        input_cnt = count_samples_in_jsonl(input_file)
        input_turn_dist = collect_turn_distribution(input_file)

        # 读取配置模板
        with open(config_file, 'r', encoding='utf-8') as f:
            config_content = f.read()

        # 替换占位符
        config_content = config_content.replace('__INPUT_FILE__', str(input_file.absolute()))
        config_content = config_content.replace('__OUTPUT_FILE__', str(output_file.absolute()))

        # 设置 work_dir
        # Data-Juicer 需要 work_dir 参数来存放临时文件和日志。
        # 如果配置文件中已存在 work_dir 行，则替换为 trace_subdir；否则在末尾添加一行。
        if 'work_dir:' in config_content:
            lines = config_content.splitlines()
            new_lines = []
            for line in lines:
                if line.strip().startswith('work_dir:'):
                    new_lines.append(f"work_dir: {trace_subdir}")
                else:
                    new_lines.append(line)
            config_content = '\n'.join(new_lines)
        else:
            config_content += f"\nwork_dir: {trace_subdir}\n"

        # 将修改后的配置内容写入临时 YAML 文件
        temp_config = Path(f"temp_{input_file.stem}.yaml")
        with open(temp_config, 'w', encoding='utf-8') as f:
            f.write(config_content)

        # 执行清洗
        cmd = f"dj-process --config {temp_config}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        # 清理临时文件
        if temp_config.exists():
            temp_config.unlink()

        if result.returncode == 0 and output_file.exists():
            success_count += 1
            output_cnt = count_samples_in_jsonl(output_file)
            output_turn_dist = collect_turn_distribution(output_file)
            print(f"    ✅ {input_file.name}: {input_cnt} → {output_cnt} 条")
            # 累加桶统计
            bucket_stats["input_samples"] += input_cnt
            bucket_stats["output_samples"] += output_cnt
            for turn, cnt in input_turn_dist.items():
                bucket_stats["input_turn_dist"][turn] += cnt
            for turn, cnt in output_turn_dist.items():
                bucket_stats["output_turn_dist"][turn] += cnt
        else:
            print(f"    ❌ {input_file.name} 清洗失败")
            if result.stderr:
                print(f"        错误: {result.stderr[:200]}")

    # 将桶统计存入 stats
    # stats 是一个字典，由调用者（main 函数）传入，用于汇总所有桶的结果。这里将当前桶的统计存入 stats["buckets"][桶名]
    # stats["buckets"][bucket_dir.name] = {
    #     "input_samples": bucket_stats["input_samples"],
    #     "output_samples": bucket_stats["output_samples"],
    #     "input_turn_dist": dict(bucket_stats["input_turn_dist"]),
    #     "output_turn_dist": dict(bucket_stats["output_turn_dist"]),
    # }

    retention_rate = 0.0
    if bucket_stats["input_samples"] > 0:
        retention_rate = bucket_stats["output_samples"] / bucket_stats["input_samples"]
    stats["buckets"][bucket_dir.name] = {
        "input_samples": bucket_stats["input_samples"],
        "output_samples": bucket_stats["output_samples"],
        "retention_rate": retention_rate,
        "input_turn_dist": dict(bucket_stats["input_turn_dist"]),
        "output_turn_dist": dict(bucket_stats["output_turn_dist"]),
    }

    return success_count

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cleaned_base = Path(CLEANED_ROOT) / timestamp
    trace_base = Path(TRACE_ROOT) / timestamp
    report_base = Path(REPORT_DIR) / timestamp
    report_base.mkdir(parents=True, exist_ok=True)

    print(f"时间戳: {timestamp}")
    print(f"清洗结果目录: {cleaned_base}")
    print(f"Trace 目录: {trace_base}")
    print(f"报告目录: {report_base}")

    overall_stats = {
        "timestamp": timestamp,
        "buckets": {},
        "total_input": 0,
        "total_output": 0,
        "overall_input_turn_dist": defaultdict(int),
        "overall_output_turn_dist": defaultdict(int),
    }


    # 对 BUCKET_CONFIG_MAP 中的每个桶，检查桶目录和配置文件是否存在。
    #调用 clean_bucket 进行清洗，传入对应的输入目录、配置文件、输出目录、trace 目录以及 overall_stats 字典（用于收集统计信息）。
    # clean_bucket 会向 overall_stats["buckets"] 中添加该桶的统计。
    total_success = 0
    for bucket_name, config_filename in BUCKET_CONFIG_MAP.items():
        bucket_dir = Path(BUCKETED_ROOT) / bucket_name
        if not bucket_dir.exists():
            continue
        config_file = Path(CONFIGS_DIR) / config_filename
        if not config_file.exists():
            print(f"⚠️ 配置文件 {config_file} 不存在，跳过桶 {bucket_name}")
            continue

        print(f"\n处理桶: {bucket_name}")
        output_dir = cleaned_base / bucket_name
        trace_dir = trace_base / bucket_name
        success = clean_bucket(bucket_dir, config_file, output_dir, trace_dir, overall_stats)
        total_success += success

        # 保存该桶的单独报告
        bucket_stats = overall_stats["buckets"][bucket_name]
        bucket_report_file = report_base / f"{bucket_name}_report.json"
        with open(bucket_report_file, 'w') as f:
            json.dump(bucket_stats, f, indent=2)
        # 绘制该桶的分布图
        plot_turn_distribution(
            bucket_name,
            bucket_stats["input_turn_dist"],
            bucket_stats["output_turn_dist"],
            report_base
        )

    # 计算全局统计
    for bucket, stats in overall_stats["buckets"].items():
        overall_stats["total_input"] += stats["input_samples"]
        overall_stats["total_output"] += stats["output_samples"]
        for turn, cnt in stats["input_turn_dist"].items():
            overall_stats["overall_input_turn_dist"][turn] += cnt
        for turn, cnt in stats["output_turn_dist"].items():
            overall_stats["overall_output_turn_dist"][turn] += cnt

    # 保存全局报告
    overall_report_file = report_base / "overall_report.json"
    # 将 defaultdict 转为 dict 以便序列化
    report_data = {
        "timestamp": overall_stats["timestamp"],
        "buckets": overall_stats["buckets"],
        "total_input": overall_stats["total_input"],
        "total_output": overall_stats["total_output"],
        "overall_input_turn_dist": dict(overall_stats["overall_input_turn_dist"]),
        "overall_output_turn_dist": dict(overall_stats["overall_output_turn_dist"]),
    }
    with open(overall_report_file, 'w') as f:
        json.dump(report_data, f, indent=2)

    # 绘制全局分布图
    plot_turn_distribution(
        "overall",
        overall_stats["overall_input_turn_dist"],
        overall_stats["overall_output_turn_dist"],
        report_base,
        selected_turns=PLOT_TURNS
    )

    # 打印汇总
    print("\n" + "="*60)
    print("清洗统计汇总")
    print("="*60)
    print(f"总体输入样本数: {overall_stats['total_input']}")
    print(f"总体输出样本数: {overall_stats['total_output']}")
    print(f"总体保留率: {overall_stats['total_output']/overall_stats['total_input']*100:.2f}%")
    print("\n各桶统计:")
    for bucket, stats in overall_stats["buckets"].items():
        inp = stats["input_samples"]
        out = stats["output_samples"]
        rate = out/inp*100 if inp>0 else 0
        print(f"  {bucket}: {inp} → {out} ({rate:.2f}%)")

    # 保存 CSV 对比文件
    csv_file = report_base / "turn_distribution_comparison.csv"
    with open(csv_file, 'w') as f:
        f.write("bucket,turn,input_count,output_count\n")
        for bucket, stats in overall_stats["buckets"].items():
            input_dist = stats["input_turn_dist"]
            output_dist = stats["output_turn_dist"]
            all_turns = set(input_dist.keys()) | set(output_dist.keys())
            for turn in sorted(all_turns):
                in_cnt = input_dist.get(turn, 0)
                out_cnt = output_dist.get(turn, 0)
                f.write(f"{bucket},{turn},{in_cnt},{out_cnt}\n")

    print(f"\n报告已保存到: {report_base}")

    # 保存桶级别汇总 CSV
    summary_csv = report_base / "bucket_summary.csv"
    with open(summary_csv, 'w', encoding='utf-8') as f:
        f.write("bucket_name,input_samples,output_samples,retention_rate\n")
        for bucket, stats in overall_stats["buckets"].items():
            inp = stats["input_samples"]
            out = stats["output_samples"]
            rate = stats.get("retention_rate", out/inp if inp>0 else 0)
            f.write(f"{bucket},{inp},{out},{rate:.6f}\n")
    print(f"桶汇总 CSV 已保存: {summary_csv}")


if __name__ == "__main__":
    main()
    

'''
关键变量作用：

overall_stats：贯穿整个脚本的统计字典，汇总所有桶的信息。

bucket_stats：在 clean_bucket 中累加单个桶的统计。

input_dist / output_dist：存储 turn 分布的字典。

success_count：记录成功清洗的文件数。

trace_subdir：每个文件独立的 trace 目录，避免不同文件运行时相互干扰。
'''