#pragma once

#include "MySQLConn.h"
#include <string>
#include <queue>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>

// 连接池，让多个线程同时写库
// 写文件和写库异步进行
class MySQLPool {
public:
    MySQLPool() {}
    ~MySQLPool() { shutdown(); }

    MySQLPool(const MySQLPool&) = delete;
    MySQLPool& operator=(const MySQLPool&) = delete;

    // 建立 pool_size 条连接
    bool init(const char* host, const char* user, const char* password,
              const char* database, unsigned int port, size_t pool_size);
    // 请求一条连接，池空时阻塞等待，直到有连接归还或池被关闭
    MySQLConn* acquire();
    // 归还一条连接
    void release(MySQLConn* conn);
    // 关闭连接池：唤醒所有等待者，销毁全部连接。
    void shutdown();

    size_t size() const { return pool_size_; }
private:
    std::queue<MySQLConn*> idle_;  // 空闲连接
    std::unordered_map<MySQLConn*, std::unique_ptr<MySQLConn>> all_; // 持有全部连接所有权
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t pool_size_ { 0 };
    std::atomic<bool> running_{ false };
};

//RAII 连接守卫：构造时从池借出，析构时自动归还
class PooledConn {
public:
    explicit PooledConn(MySQLPool& pool)
        : pool_(pool), conn_(pool.acquire()) {}
    ~PooledConn() {
        if (conn_) pool_.release(conn_);
    }

    PooledConn(const PooledConn&) = delete;
    PooledConn& operator=(const PooledConn&) = delete;

    explicit operator bool() const { return conn_ != nullptr; }
    MySQLConn* operator->() const { return conn_; }
    MySQLConn& operator*() const { return *conn_; }
    MySQLConn* get() const { return conn_; }
private:
    MySQLPool& pool_;
    MySQLConn* conn_;
};