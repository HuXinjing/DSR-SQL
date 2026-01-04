# DSR-SQL 中文使用指南

## 📖 项目简介

DSR-SQL 是一个基于双状态推理（Dual-State Reasoning）的 Text-to-SQL 框架，支持多种数据库类型，包括 SQLite、Snowflake、BigQuery、MySQL 和 **Doris**。

本项目实现了自适应上下文状态和渐进式生成状态的交互，能够处理复杂的企业级数据库查询任务。

## 🚀 快速开始

### 1. 环境配置

#### 1.1 安装 Python 依赖

```bash
cd DSR_Lite
pip install -r requirements.txt
```

**主要依赖包：**
- `pymysql==1.1.1` - MySQL/Doris 数据库连接
- `snowflake-connector-python` - Snowflake 连接
- `google-cloud-bigquery` - BigQuery 连接
- `deepseek` - LLM 模型调用

#### 1.2 配置 LLM API

确保已配置 LLM API 密钥（DeepSeek、OpenAI 等），相关配置在 `LLM/LLM_OUT.py` 中。

### 2. 数据库配置

#### 2.1 配置文件位置

所有数据库配置都在 `DSR_Lite/utils/DBsetup/DB.json` 文件中。

#### 2.2 SQLite 配置

SQLite 是本地文件数据库，无需额外配置：

```json
{
    "DB_type": "Sqlite",
    "Local_path": "spider2-lite/resource/databases/sqlite",
    "Authentication": "no have"
}
```

#### 2.3 MySQL 配置

**步骤 1：创建 MySQL 认证文件**

在项目根目录创建 `mysql_credential.json` 文件：

```json
{
    "host": "your_mysql_ip",
    "port": 3306,
    "user": "your_username",
    "password": "your_password",
    "database": "your_database_name"
}
```

**示例：**
```json
{
    "host": "192.168.1.100",
    "port": 3306,
    "user": "root",
    "password": "mypassword",
    "database": "test_db"
}
```

**步骤 2：配置 DB.json**

在 `DSR_Lite/utils/DBsetup/DB.json` 中配置：

```json
{
    "DB_type": "MySQL",
    "Local_path": "spider2-lite/resource/databases/mysql",
    "Authentication": "mysql_credential.json"
}
```

**说明：**
- `Local_path`: Schema JSON 文件的存储路径（可选，用于存储生成的 schema 文件）
- `Authentication`: MySQL 连接凭证文件路径（必需）

#### 2.4 Doris 配置（重点）

Doris 是 Apache Doris（原 Palo），一个高性能的实时分析数据库。配置方式与 MySQL 类似，但端口和连接方式有所不同。

**步骤 1：创建 Doris 认证文件**

在项目根目录创建 `doris_credential.json` 文件：

```json
{
    "host": "your_doris_fe_ip",
    "port": 9030,
    "user": "your_username",
    "password": "your_password",
    "database": "your_database_name"
}
```

**重要参数说明：**
- `host`: Doris FE（Frontend）节点的 IP 地址
- `port`: **9030**（Doris 的 MySQL 协议端口，不是 3306）
- `user`: Doris 用户名（通常是 `root`）
- `password`: Doris 用户密码
- `database`: 要连接的数据库名称

**示例配置：**
```json
{
    "host": "192.168.69.51",
    "port": 9030,
    "user": "root",
    "password": "",
    "database": "zhiyuan_ies"
}
```

**步骤 2：配置 DB.json**

在 `DSR_Lite/utils/DBsetup/DB.json` 中配置：

```json
{
    "DB_type": "Doris",
    "Local_path": "spider2-lite/resource/databases/doris",
    "Authentication": "doris_credential.json"
}
```

**Doris 特殊说明：**
1. **端口号**：Doris 使用 **9030** 端口（MySQL 协议），不是标准的 3306
2. **连接方式**：Doris 兼容 MySQL 协议，所以使用 `pymysql` 连接
3. **FE 节点**：确保 `host` 指向的是 Doris FE（Frontend）节点，不是 BE（Backend）节点
4. **网络连通性**：确保运行环境能够访问 Doris FE 节点的 9030 端口

**验证 Doris 连接：**
```bash
# 使用 MySQL 客户端测试连接
mysql -h your_doris_fe_ip -P 9030 -u root -p
```

#### 2.5 Snowflake 配置

```json
{
    "DB_type": "Snowflake",
    "Local_path": "spider2-lite/resource/databases/snowflake",
    "Authentication": "spider2-lite/evaluation_suite/snowflake_credential.json"
}
```

#### 2.6 BigQuery 配置

```json
{
    "DB_type": "Bigquery",
    "Local_path": "spider2-lite/resource/databases/bigquery",
    "Authentication": "spider2-lite/evaluation_suite/bigquery_credential.json"
}
```

### 3. 数据预处理（Schema 生成）

在运行 Text-to-SQL 之前，需要先生成数据库的 Schema JSON 文件。

#### 3.1 执行预处理脚本

```bash
cd DSR_Lite
bash script/preprocess.sh
```

**预处理脚本说明：**

该脚本会依次处理不同类型的数据库：

1. **SQLite**：直接从数据库文件提取 schema
2. **BigQuery**：需要 LLM 分析表命名规则
3. **Snowflake**：需要 LLM 分析表命名规则
4. **MySQL**：连接数据库提取 schema，支持 LLM 分析相似表名
5. **Doris**：连接数据库提取 schema，支持 LLM 分析相似表名

#### 3.2 单独处理 MySQL/Doris

如果需要单独处理 MySQL 或 Doris：

```bash
# 处理 MySQL（使用默认 LLM 模型 deepseek-chat）
python -m utils.preprocessor.Get_table_mes_mysql --db_type mysql --db_name your_db_name

# 处理 Doris（使用默认 LLM 模型 deepseek-chat）
python -m utils.preprocessor.Get_table_mes_mysql --db_type doris --db_name your_db_name

# 覆盖已存在的 schema 文件
python -m utils.preprocessor.Get_table_mes_mysql --db_type doris --db_name your_db_name --overwrite

# 指定 LLM 模型（用于分析相似表名的命名规则）
python -m utils.preprocessor.Get_table_mes_mysql --db_type doris --db_name your_db_name --model deepseek-chat

# 使用其他 LLM 模型（如 OpenAI GPT-4）
python -m utils.preprocessor.Get_table_mes_mysql --db_type doris --db_name your_db_name --model gpt-4
```

**参数说明：**
- `--db_type`: 数据库类型，`mysql` 或 `doris`（必需）
- `--db_name`: 数据库名称（可选，默认使用 credential 文件中的 database）
- `--overwrite`: 覆盖已存在的 schema JSON 文件（可选）
- `--model`: LLM 模型名称（可选，默认值：`deepseek-chat`）

**LLM 参数说明：**
- **默认值**：如果不指定 `--model`，默认使用 `deepseek-chat`
- **何时需要 LLM**：
  - 当数据库中有**相似表名**（如哈希分表、日期分表）且**表数量 > 5** 时，会调用 LLM 分析命名规则
  - 如果表数量 ≤ 5，会使用模板生成描述，**不需要 LLM**
  - 如果数据库中没有相似表名，**不需要 LLM**
- **LLM 失败处理**：如果 LLM 调用失败，会自动回退到模板描述，不会中断处理流程
- **推荐模型**：`deepseek-chat`（默认）、`gpt-4`、`gpt-3.5-turbo` 等

**预处理输出：**

预处理完成后，会在 `Local_path` 指定的目录下生成 `{db_name}_M-Schema.json` 文件，包含：
- 表结构信息
- 列信息（名称、类型、示例值）
- 外键关系
- 相似表的分组信息（如果有）

### 4. 运行 Text-to-SQL

#### 4.1 基本运行

```bash
cd DSR_Lite
python main_lite.py
```

#### 4.2 运行参数

查看完整参数：

```bash
python main_lite.py --help
```

**主要参数：**
- `--input_path`: 输入问题文件路径
- `--output_path`: 输出结果文件路径
- `--db_type`: 数据库类型（sqlite/snowflake/bigquery/mysql/doris）
- `--model`: LLM 模型名称
- `--max_token`: Schema 最大 token 数

**示例：**
```bash
# 使用 Doris 数据库
python main_lite.py \
    --input_path data/input.json \
    --output_path results/output.json \
    --db_type doris \
    --model deepseek-chat
```

## 🔧 常见问题

### Q1: Doris 连接失败

**问题：** `pymysql.err.OperationalError: (2003, "Can't connect to MySQL server")`

**解决方案：**
1. 检查 `doris_credential.json` 中的 `host` 和 `port` 是否正确
2. 确认端口是 **9030**（不是 3306）
3. 检查网络连通性：`telnet your_doris_ip 9030`
4. 确认防火墙规则允许访问 9030 端口
5. 验证 FE 节点是否正常运行

### Q2: MySQL/Doris Schema 生成失败

**问题：** 预处理时无法生成 schema JSON 文件

**解决方案：**
1. 检查数据库连接凭证是否正确
2. 确认数据库用户有足够的权限（需要 `SELECT` 权限访问 `INFORMATION_SCHEMA`）
3. 检查数据库名称是否正确
4. 查看错误日志，确认具体失败原因

### Q3: 相似表名分析失败

**问题：** LLM 调用失败，无法分析相似表名的命名规则

**解决方案：**
1. **检查 LLM API 配置**：确认 `LLM/LLM_OUT.py` 中的 API 配置正确
2. **检查网络连接**：确认可以访问 LLM API 服务
3. **自动回退机制**：如果 LLM 调用失败，系统会自动使用模板描述，不会中断处理
4. **更换模型**：可以设置 `--model` 参数使用其他 LLM 模型
   ```bash
   python -m utils.preprocessor.Get_table_mes_mysql --db_type doris --model gpt-4
   ```
5. **跳过 LLM**：如果表数量 ≤ 5，系统会自动使用模板，无需 LLM
6. **检查日志**：查看控制台输出的错误信息，确认具体失败原因

### Q4: 预处理脚本执行很慢

**问题：** 预处理 MySQL/Doris 数据库时耗时很长

**解决方案：**
1. 这是正常现象，特别是当数据库表很多时
2. 如果表数量很多，LLM 分析相似表名会消耗较长时间
3. 可以通过 `--model` 参数使用更快的模型
4. 对于单表数据库，可以跳过相似表名分析

### Q5: 如何查看生成的 Schema 文件

**位置：** `{Local_path}/{db_name}/{db_name}_M-Schema.json`

**文件结构：**
```json
{
    "database_name": {
        "table1": [
            ["column1", "Primary Key", "INT", null, "example1, example2"],
            ["column2", null, "TEXT", null, "example3, example4"]
        ],
        "table2": [...]
    },
    "foreign_keys": {
        "table1.column1": "table2.column2"
    },
    "table_Information": {
        "table1": ["table1_202401", "table1_202402"]
    },
    "table_description_summary": {
        "table1": "Tables with similar structure, representing monthly data..."
    }
}
```

## 📝 文件结构说明

```
DSR_Lite/
├── main_lite.py              # 主程序入口
├── requirements.txt          # Python 依赖
├── script/
│   └── preprocess.sh        # 预处理脚本
├── utils/
│   ├── DBsetup/
│   │   ├── DB.json          # 数据库配置文件
│   │   └── Get_DB.py        # 配置读取
│   ├── Database_Interface.py  # 数据库接口（连接、查询）
│   ├── preprocessor/
│   │   ├── Get_table_mes_mysql.py  # MySQL/Doris Schema 生成
│   │   ├── Get_table_mes_sqlite.py # SQLite Schema 生成
│   │   ├── Get_table_mes_bigquery.py # BigQuery Schema 生成
│   │   └── Get_table_mes_snow.py    # Snowflake Schema 生成
│   └── ...
└── ...
```

## 🎯 使用流程总结

1. **安装依赖**：`pip install -r requirements.txt`
2. **配置数据库**：创建 `mysql_credential.json` 或 `doris_credential.json`
3. **更新 DB.json**：配置数据库路径和认证文件
4. **运行预处理**：`bash script/preprocess.sh` 生成 Schema JSON
5. **运行主程序**：`python main_lite.py` 执行 Text-to-SQL

## 📞 技术支持

如有问题，请提交 Issue 或联系项目维护者。

---

**注意：** 本指南重点介绍了 Doris 和 MySQL 的配置，因为它们是新增支持的数据库类型。其他数据库（SQLite、Snowflake、BigQuery）的配置请参考原始文档。

