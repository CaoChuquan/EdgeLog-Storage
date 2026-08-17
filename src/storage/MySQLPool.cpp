#include "MySQLPool.h"

bool MySQLPool::init(const char* host, const char* user, const char* password,
          const char* database, unsigned int port, size_t pool_size)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
        return true;
    }
    if (pool_size == 0) {
        pool_size = 1;
    }

    // 建立 pool_size 条连接
    for (size_t i = 0; i < pool_size; ++i) {
        auto conn = std::make_unique<MySQLConn>();
        if (!conn->connect(host, user, password, database, port)) {
            // 任一条连接建立失败即整体失败，清空已建连接后返回
            idle_ = {};
            all_.clear();
            return false;
        }
        MySQLConn* raw = conn.get();
        all_.emplace(raw, std::move(conn));
        idle_.push(raw);
    }

    pool_size_ = pool_size;
    running_ = true;
    return true;
}

MySQLConn* MySQLPool::acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    // 池空则阻塞等待归还；池关闭时唤醒并返回 nullptr
    cv_.wait(lock, [this] { return !idle_.empty() || !running_; });
    if (!running_) {
        return nullptr;
    }
    MySQLConn* conn = idle_.front();
    idle_.pop();
    return conn;
}

void MySQLPool::release(MySQLConn* conn) {
    if (!conn) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    // 仅归还本池持有的连接
    if (all_.find(conn) == all_.end()) {
        return;
    }
    idle_.push(conn);
    cv_.notify_one();
}

void MySQLPool::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ && all_.empty()) {
        return;
    }
    running_ = false;
    cv_.notify_all();   // 唤醒所有等待者，让它们拿到 nullptr 退出
    idle_ = {};
    all_.clear();       // 销毁全部连接（unique_ptr 析构触发 mysql_close）
    pool_size_ = 0;
}