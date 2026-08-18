#pragma once

#include "MySQLPool.h"
#include <string>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include <thread>
#include <atomic>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>

/*
存储层门面：把"收到的一条设备日志"真正落地。

它做两件事，合起来是"文件在磁盘、账本在数据库"：
    1. 把日志内容按设备追加写入本地文件（deep_storage/<device_id>/<device_id>.log）。
    2. 在 MySQL 里维护该文件的元数据（files 表）与设备信息（devices 表）。
 */
class StorageManager {
public:
    static StorageManager& instance() { 
        static StorageManager ins;
        return ins;
    }

    // 从 Config 读取 mysql_* 与 deep_storage_dir 等参数，建连接池、落盘根目录，
    // 并启动一组后台 flush 工作线程。成功返回 true。
    bool init();

    // 停止全部 flush 线程（先冲刷剩余批次），再关闭连接池。可安全重复调用。
    void shutdown();

    // 存储一条来自某设备的日志：先做 seq 幂等去重，再同步落盘 + 异步投递元数据入库
    // seq 为设备本地单调递增序号（弱网重传时可能重复）：
    //   - seq 大于该设备已处理过的最大 seq  -> 视为新日志，正常落盘；
    //   - seq 小于等于已处理过的最大 seq     -> 视为重传的重复日志，跳过落盘
    // 两种情况都返回 true，让网络层给设备回 ACK（重复帧也要 ACK，才能让设备清缓存）
    // seq==0 表示老格式/不可靠帧，不参与去重，一律落盘。
    // 线程安全，可被网络层线程池并发调用。落盘失败返回 false（不回 ACK，触发重传）
    bool store(const std::string& device_id, uint64_t seq, const std::string& log_data);

    bool isReady() const { return ready_; }

private:
    StorageManager() = default;

    // 一条待写入 files 表的文件元数据（纯数据，不含任何 SQL）
    // 由 store() 生产、投递进 pending_，由 flush 线程消费。
    struct FileMeta {
        std::string device_id;      // 设备标识（当前为来源 IP）
        std::string storage_path;   // 落盘文件的本地路径
        std::string url;            // 下载路径，同时是 files 表唯一键
        std::int64_t fsize{ 0 };    // 追加写后从磁盘 stat 到的文件大小
        std::int64_t mtime{ 0 };    // 文件修改时间（epoch 秒）
        std::int64_t atime{ 0 };    // 文件访问时间（epoch 秒）
        std::string file_hash;      // 文件内容哈希（一致性/去重线索）
    };

    // 计算内容哈希（16 位十六进制字符串，写入 files.file_hash）
    std::string hashContent(const std::string& content) const;

    // flush 工作线程主循环：循环地从 pending_ 取一批元数据并写库，直到停机
    // 多个线程同时跑这个函数，共享 pending_ 队列
    void flushLoop();

    // 把一批元数据用多值 INSERT 落库（含 devices 维护），借用连接池的连接
    // 由 flush 线程调用；每次调用内部独占一条连接。
    bool flushBatch(std::vector<FileMeta>& batch);

    MySQLPool pool_;                 // MySQL 连接池
    std::string storage_dir_;        // 落盘根目录，末尾带 '/'
    std::string download_prefix_;    // url 前缀，用于生成下载路径

    // 落盘串行化：仅保护文件追加写，不参与写库
    std::mutex file_mutex_;

    std::deque<FileMeta> pending_;         // 待写入的元数据
    std::mutex queue_mutex_;               // 保护 pending_
    std::condition_variable queue_cv_;     // 队列非空/停机时唤醒 flush 线程
    std::vector<std::thread> flush_threads_;  // 一组 flush 工作线程

    // ---- seq 幂等去重 ----
    // 目的：弱网重传会让同一 seq 到达多次，需要把"已成功处理过的 seq"挡在落盘之前
    //
    // 难点：网络层把每条日志的回调投递到线程池，多个线程并发调用 store()，
    // 因此同一设备的 seq 会乱序到达（seq=2 可能先于 seq=1 被处理）。
    // 若只用"已见过的最大 seq"做判断，后到的较小 seq 会被误判成重复而丢弃——这是错的。
    //
    // 方案（借鉴 TCP SACK 的水位 + 空洞集合）：每个设备维护
    //   - contiguous：已连续处理到的最大 seq（1..contiguous 都已处理，形成"水位"）；
    //   - ahead：水位之上、已处理但尚不连续的 seq 集合（乱序提前到达的"空洞之上"部分）
    // 判重规则：seq<=contiguous 或 seq∈ahead  => 重复；否则是新日志。
    // 处理新日志后把 seq 并入，并尽可能把 ahead 里紧邻 contiguous 的 seq 合并进水位，
    // 使 ahead 只保留真正的乱序缺口，集合规模有界（正常顺序时始终为空）。
    struct DedupState {
        uint64_t contiguous{ 0 };               // 连续水位：1 ~ n 均已处理
        std::unordered_set<uint64_t> ahead;     // 水位之上、乱序已处理的 seq
    };
    std::unordered_map<std::string, DedupState> dedup_;
    std::mutex dedup_mutex_;                     // 保护 dedup_

    // 判断 (device_id, seq) 是否为重复帧。非重复时"预占"该 seq（并入去重状态），
    // 通过出参 prev_contiguous / inserted_ahead 记录预占方式，供落盘失败时回滚。
    // 返回 true 表示重复（调用方应跳过落盘、直接回 ACK）
    bool isDuplicateAndReserve(const std::string& device_id, uint64_t seq,
                               uint64_t& prev_contiguous, bool& inserted_ahead);
    // 落盘失败时回滚 isDuplicateAndReserve 的预占，让该 seq 能被重传
    void rollbackReserve(const std::string& device_id, uint64_t seq,
                         uint64_t prev_contiguous, bool inserted_ahead);

    size_t flush_threads_num_{ 4 };    // flush 工作线程数（一般对齐连接池大小）
    size_t batch_size_{ 100 };         // 攒够多少条触发一次写库
    int batch_interval_ms_{ 200 };     // 或超过多少毫秒触发一次
    int max_deadlock_retry_{ 5 };      // 遇到 InnoDB 死锁时整批事务的最大重试次数

    std::atomic<bool> ready_{ false };    // 是否可接收 store
    std::atomic<bool> running_{ false };  // flush 线程是否应继续运行
};
