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

    // 存储一条来自某设备的日志：同步落盘 + 异步投递元数据入库。
    // 线程安全，可被网络层线程池并发调用。返回 false 表示落盘失败。
    bool store(const std::string& device_id, const std::string& log_data);

    bool isReady() const { return ready_; }

private:
    StorageManager() = default;

    // 一条待写入 files 表的文件元数据（纯数据，不含任何 SQL）。
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

    // flush 工作线程主循环：循环地从 pending_ 取一批元数据并写库，直到停机。
    // 多个线程同时跑这个函数，共享 pending_ 队列。
    void flushLoop();

    // 把一批元数据用多值 INSERT 落库（含 devices 维护），借用连接池的连接。
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

    size_t flush_threads_num_{ 4 };    // flush 工作线程数（一般对齐连接池大小）
    size_t batch_size_{ 100 };         // 攒够多少条触发一次写库
    int batch_interval_ms_{ 200 };     // 或超过多少毫秒触发一次
    int max_deadlock_retry_{ 5 };      // 遇到 InnoDB 死锁时整批事务的最大重试次数

    std::atomic<bool> ready_{ false };    // 是否可接收 store
    std::atomic<bool> running_{ false };  // flush 线程是否应继续运行
};
