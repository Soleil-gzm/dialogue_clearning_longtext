# 多轮对话数据清洗项目
## 项目目标
本项目用于处理银行催收场景的多轮对话数据，将原始 JSON 格式的对话拆分为多轮样本，按轮次分桶，使用 Data-Juicer 进行差异化清洗，并生成清洗报告和可视化图表、带有 `loss` 标记的训练数据 JSON 文件。

## 项目结构
.  
├── data/ # 原始数据目录  
│         └── data-record-processed-92049-filter.json  
├── configs/ # Data-Juicer 配置文件（按桶分）  
│         ├── config_bucket_turn0.yaml  
│         ├── config_bucket_turn1.yaml  
│         ├── ...  
│         └── overal_config.yaml  
├── scripts/ # Data-Juicer 配置文件（按桶分）
│ 		├──  01_split_dialogues.py  
│         ├── 02_split_into_buckets.py  
│         ├── 03_clean_buckets_with_plots.py  
│         ├── 04_apply_cleaned_loss_direct.py  
│         └── run_pipeline.sh # 一键运行脚本（见下文）  
├── samples/ # 拆分后的样本（中间产物）  
├── bucketed/ # 分桶后的样本  
├── cleaned_jsonl/ # 清洗后的样本（带时间戳）  
├── trace_output/ # Data-Juicer 运行时日志  
├── cleaning_reports/ # 清洗报告（JSON + 图表）  
├── final_training_data/ # 最终训练 JSON 文件  
└── progress.txt # 断点续传记录

## 环境配置
项目依赖 conda 环境：

>     data-juicer：用于数据清洗 
>     

## 数据准备

1.  将原始对话 JSON 文件放置于 `data/` 目录下（或修改脚本中的 `INPUT_JSON` 路径）。
    
2.  根据业务需要，为每个桶编写 Data-Juicer 配置文件，存放于 `configs/` 目录。
    
    -   配置文件中需包含 `dataset_path`（将被脚本中的占位符 `__INPUT_FILE__` 替换）、`export_path`（`__OUTPUT_FILE__`）以及 `work_dir`（脚本会自动注入）。
        
    -   示例配置见 `configs/overal_config.yaml`。

## 运行流程
### 方式一：分步执行（便于调试）

    # Step 1: 拆分为样本
    python 01_split_dialogues.py
    
    # Step 2: 按轮次分桶
    python 02_split_into_buckets.py
    
    # Step 3: 清洗每个桶（注意修改 BUCKET_CONFIG_MAP 中的配置文件映射）
    python 03_clean_buckets_with_plots.py
    
    # Step 4: 应用清洗结果，生成最终训练 JSON
    python 04_apply_cleaned_loss_direct.py
### 方式二：一键运行（推荐）

     run_pipeline.sh
## 配置说明

### 01_split_dialogues.py

    INPUT_JSON：原始 JSON 文件路径
    OUTPUT_DIR：样本输出目录（samples）
    BATCH_SIZE：每个 JSONL 文件包含的对话数（默认 120000 ，根据电脑配置来决定）

### 02_split_into_buckets.py

    BUCKETS字典：定义 (low, high) 到桶名的映射，可根据需要修改。
    

### 03_clean_buckets_with_plots.py

    BUCKET_CONFIG_MAP： 桶名 → 配置文件名（位于 configs/ 下）
    PLOT_TURNS ：全局图中展示的轮次范围（ None 表示全部）
    
    -   清洗后自动生成：
    -   cleaning_reports/<timestamp>/overall_report.json
    -   cleaning_reports/<timestamp>/bucket_*_report.json   
    -   cleaning_reports/<timestamp>/turn_distribution_comparison.csv  
    -   各桶及全局的轮次分布柱状图
### 04_apply_cleaned_loss_direct.py

    ---original：原始 JSON 路径
    ---cleaned_root：清洗结果根目录（默认 cleaned_jsonl）
    ---timestamp：指定时间戳（默认使用最新）   
    ---unwashed_buckets：未清洗的桶名列表（这些桶的全部样本将被保留）
    ---bucket_turn_range：为未清洗桶指定 turn 范围（格式：bucket_name low high）

## 注意事项

1.  **断点续传**：`01_split_dialogues.py` 支持中断后继续处理，进度记录在 `progress.txt`。
    
2.  **Data-Juicer 配置**：确保配置文件中的算子与数据格式兼容。本脚本使用占位符替换输入/输出路径，配置文件中可保留通用算子。
    
3.  **磁盘空间**：拆分后的样本数量巨大（原始对话数 × 平均轮次），请预留足够空间（建议 50GB+）。
    
4.  **并行清洗**：`03_clean_buckets_with_plots.py` 目前串行处理每个桶内的每个文件，若桶内文件较多，可考虑改为多进程。
    
5.  **loss 字段类型**：最终输出的 `loss` 值为字符串 `"True"`/`"False"`，训练时需转换为布尔值。
    

## 常见问题

**Q: 清洗脚本报错 `dj-process: command not found`**  
A: 请确保 Data-Juicer 已正确安装，且 `dj-process` 在 PATH 中。

**Q: 如何修改某个桶的清洗策略？**  
A: 编辑 `configs/` 下对应的 YAML 文件，修改算子列表或参数即可。

**Q: 清洗报告中的 `retention_rate` 是如何计算的？**  
A: `输出样本数 / 输入样本数`，表示该桶中样本的保留比例。

**Q: 能否只重新清洗某个桶？**  
A: 可以。删除 `cleaned_jsonl/<timestamp>/<bucket_name>` 目录，然后重新运行 `03_clean_buckets_with_plots.py`（注意会自动创建新的时间戳目录，如需覆盖可手动指定时间戳）。

常见问题
Q1: 清洗脚本报错 dj-process: command not found
A: 请确保已激活 Data-Juicer 环境：conda activate data_juicer。

Q2: 分桶时提示 IndentationError
A: 检查 split_into_buckets.py 中的缩进，确保使用空格而非 Tab，参考仓库中的正确版本。

Q3: 清洗后某些桶输出为空
A: 检查该桶的配置文件路径是否正确，以及输入文件是否包含对应 turn 的样本。

Q4: 如何重新清洗（修改配置后）？
A: 重新运行 clean_buckets_with_plots.py 会自动生成新的时间戳目录，不会覆盖历史结果。

扩展与自定义
添加新算子：修改对应桶的 YAML 配置文件即可。

调整分桶策略：编辑 split_into_buckets.py 中的 BUCKETS 列表。

增加更多统计指标：可在 clean_buckets_with_plots.py 中扩展 collect_turn_distribution 函数。



作者
根据实际需求编写，如有问题请联系项目维护者。

注意：本 README 对应项目结构为当前清洗流程的最新版本。如有文件路径或脚本名称变动，请相应调整。

text

你可以将此内容保存为 `README.md`，并根据实际脚本文件名微调（例如你的清洗脚本可能叫 `clean_buckets_with_plots.py`，分桶脚本叫 `split_into_buckets.py` 等）。
