# EdgeLog Storage

面向边缘设备的**可靠日志采集与存储系统**。设备端 SDK 在弱网/掉电场景下保证日志不丢，
服务端高并发接收日志，正文按设备分文件落盘、元数据入 MySQL，并在高并发写入下保证零丢失。

- **语言 / 环境**：C++17、epoll、多线程、MySQL、CMake、Linux
- **定位**：写侧链路完整（采集 → 可靠传输 → 幂等去重 → 落盘 → 元数据入库），覆盖弱网、进程崩溃、高并发死锁等边界。

---

## 特性

| 方向 | 说明 |
|------|------|
| 高并发网络接入 | 单 Reactor：epoll 边缘触发 + eventfd 唤醒 + 线程池处理业务回调；eventfd 主动唤醒替代超时轮询，支持即时优雅停机 |
| 存储分层 | 日志正文按设备分文件落盘（`deep_storage/<device_id>/`），元数据入 MySQL；`MySQLConn`（RAII 连接）+ `StorageManager`（单例门面）两层解耦 |
| 并发写入优化 | MySQL 连接池 + 多 flush 线程并发攒批写入；`INSERT ... ON DUPLICATE KEY UPDATE` 避免表膨胀，JOIN 批量更新替代逐条子查询 |
| 死锁处理与零丢失 | 批内数据排序统一加锁顺序 + 指数退避重试；重试耗尽的死锁批次**重新入队**而非丢弃，保证高并发下零丢失 |
| 弱网可靠性 | 应用层 per-seq ACK + 设备端 WAL（先落盘 fsync 再发送）；断线自动重连重传、进程崩溃后回放恢复 |
| 幂等去重 | 服务端 TCP-SACK 式「连续水位 + 乱序集合」去重，正确处理线程池并发导致的 seq 乱序到达 |
| WAL compaction | 阈值触发重写 + `rename` 原子替换，日志文件体积从线性增长收敛至与在途数据相当 |
| 显式设备标识 | 连接级 `HELLO <device_id>` 握手携带设备标识，替代以客户端 IP 标识，规避 NAT/换 IP 混淆；无握手时退回 IP（向后兼容） |

---

## 性能与可靠性（实测）

测试环境：单机，MySQL 8.4 @ `127.0.0.1`，`pool_size=10`、`flush_threads=8`、`batch=100`。
压测样本：200 台设备 × 20 条 = 4000 条日志，8 线程并发写入。

| 指标 | 结果 |
|------|------|
| flush 优化效果 | 批量落盘耗时 **1022ms → 58~124ms（约 10×）**（单连接+全局锁 → 连接池+多 flush 线程） |
| 并发写入吞吐 | **7400~9200 logs/s**（写入流水线） |
| 数据可靠性 | **零丢失**：连续 8 次压测均 200/200 落库 |
| 死锁零丢失验证 | 强制 `db_max_deadlock_retry=0` 复现死锁，触发 8 次批次重入队，仍 **200/200 零丢失** |
| 元数据入库 | JOIN 批量更新比逐条子查询减少约 **2.8×** 数据库往返 |
| 弱网可靠性 | 20 条日志经 2 次断线重连，最终恰好一次送达（去重后 unique=20，帧数 27 > 20 证明触发了重传） |
| WAL compaction | 500 条日志全部确认后，WAL 文件体积由约 **15KB 收敛至 6 字节** |

> 说明：数据均由仓库内 `test_storage_bench` / `test_reliable` 实测得出，可复现（见下方测试）。

---

## 目录结构

```
edgelog_new/
├── src/
│   ├── config/       # 配置加载（Config 单例，读取 config.json）
│   ├── log/          # 异步日志（AsyncLogger）
│   ├── net/          # 网络层（epoll + eventfd + 线程池，Server）
│   ├── threadpool/   # 线程池
│   ├── storage/      # 存储层（MySQLConn / MySQLPool / StorageManager）
│   ├── device/       # 设备端可靠上报 SDK（ReliableClient：WAL + ACK + 重传）
│   └── main.cpp      # 服务端入口
├── tests/            # 端到端测试
├── third_party/      # 第三方头文件（JSON 等）
├── config.json       # 运行配置
└── CMakeLists.txt
```

---

## 协议

设备与服务端基于文本行协议（每帧以 `\n` 结尾）：

| 帧 | 方向 | 含义 |
|----|------|------|
| `HELLO <device_id>` | 设备 → 服务端 | 连接建立后声明设备标识，绑定到该连接（重连需重发） |
| `LOG <seq> <payload>` | 设备 → 服务端 | 可靠模式日志，服务端处理成功后回 ACK |
| `ACK <seq>` | 服务端 → 设备 | 确认 seq 已可靠持久化，设备据此清理 WAL |
| 任意文本（无 `LOG ` 前缀） | 设备 → 服务端 | 兼容老格式，按 seq=0 普通日志处理，不回 ACK |

设备端 WAL 文件格式（append-only，每行一条记录）：

```
P <seq> <escaped_payload>    # PUT：产生一条日志
A <seq>                      # ACK：seq 已确认
N <next_seq>                 # next-seq 水位（compaction 写入，防止重启后 seq 复用）
```

---

## 构建

### 依赖

- CMake ≥ 3.12、支持 C++17 的编译器（g++ / clang++）
- MySQL C 客户端开发库：

```bash
sudo apt update
sudo apt install libmysqlclient-dev
```

### 编译

```bash
cmake -S . -B build
cmake --build build -j
```

产物输出到 `bin/`：`edgelog-storage`（服务端）及各测试可执行文件。

---

## 数据库初始化

代码不自动建表，需先手动创建库与表（字段与 `StorageManager` 落库逻辑对齐）：

```sql
CREATE DATABASE IF NOT EXISTS edgelog
  DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE edgelog;

CREATE TABLE `files` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT,
  `mtime`        BIGINT       NOT NULL,
  `atime`        BIGINT       NOT NULL,
  `fsize`        BIGINT       NOT NULL,
  `storage_path` VARCHAR(1024) NOT NULL,
  `url`          VARCHAR(512)  NOT NULL,
  `file_hash`    VARCHAR(32)   NOT NULL,
  `device_id`    VARCHAR(128)  DEFAULT '',
  `storage_type` TINYINT       DEFAULT 0,
  `created_at`   TIMESTAMP     NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `url` (`url`),
  KEY `idx_hash` (`file_hash`),
  KEY `idx_device` (`device_id`),
  KEY `idx_created_at` (`created_at`)
) ENGINE = InnoDB;

CREATE TABLE `devices` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT,
  `device_id`   VARCHAR(128) NOT NULL,
  `device_name` VARCHAR(256) DEFAULT '',
  `last_active` TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
  `file_count`  INT          DEFAULT 0,
  `created_at`  TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `device_id` (`device_id`)
) ENGINE = InnoDB;
```

---

## 配置

运行配置见 `config.json`：

| 键 | 默认值 | 说明 |
|----|--------|------|
| `port` | `8080` | 服务端监听端口 |
| `log_dir` / `log_level` | `./logs` / `DEBUG` | 服务端自身日志目录与级别 |
| `deep_storage_dir` | `./deep_storage/` | 设备日志正文落盘根目录 |
| `mysql_host` / `mysql_port` | `127.0.0.1` / `3306` | MySQL 地址 |
| `mysql_user` / `mysql_password` / `mysql_database` | `root` / — / `edgelog` | MySQL 账号与库名 |
| `mysql_pool_size` | `10` | 连接池大小 |
| `db_flush_threads` | `8` | flush 线程数（建议与连接池大小相当） |
| `db_batch_size` | `100` | 单批攒批条数 |
| `db_batch_interval_ms` | `200` | 攒批最长等待时间 |
| `db_max_deadlock_retry` | `5` | 单批死锁重试上限；耗尽后重入队而非丢弃 |

---

## 运行

```bash
./bin/edgelog-storage        # 读取同目录 config.json，监听 config 中的 port
```

设备端可靠上报通过 `ReliableClient`（`src/device/`）集成到设备程序中，最小用法：

```cpp
ReliableClient client;
// wal 路径、服务端地址、设备号、compaction 阈值（默认 1000，传 0 关闭）
client.start("./device.wal", "127.0.0.1", 8080, "device-001");
client.log("hello from edge");   // 落盘 WAL 后台可靠上报，断网/崩溃不丢
// ...
client.stop();
```

---

## 测试

```bash
cmake --build build -j

# 不依赖 MySQL：弱网可靠性端到端（WAL + ACK + 断线重传 + 崩溃恢复 + compaction）
./bin/test_reliable

# 依赖 MySQL（需先完成数据库初始化）
./bin/test_network         # 网络层收发与向后兼容
./bin/test_storage         # 存储层落盘 + 元数据入库
./bin/test_storage_bench   # 并发写入压测（吞吐 / 零丢失）

# 或统一用 ctest
ctest --test-dir build --output-on-failure
```