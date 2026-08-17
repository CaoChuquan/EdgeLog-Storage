#include "StorageManager.h"
#include "Config.h"
#include "AsyncLogger.h"

#include <filesystem>
#include <fstream>
#include <sys/stat.h>
#include <ctime>
#include <cstdio>
#include <chrono>
#include <sstream>
#include <unordered_set>
#include <algorithm>
#include <thread>

namespace fs = std::filesystem;

bool StorageManager::init() {
    // 1. 从配置读取参数（字段与 config/Storage.conf 对齐）
    std::string host = Config::instance().getString("mysql_host", "127.0.0.1");
    int port = Config::instance().getInt("mysql_port", 3306);
    std::string user = Config::instance().getString("mysql_user", "root");
    std::string pass = Config::instance().getString("mysql_password", "");
    std::string db = Config::instance().getString("mysql_database", "edgelog");
    int pool_size = Config::instance().getInt("mysql_pool_size", 10);

    storage_dir_     = Config::instance().getString("deep_storage_dir", "./deep_storage/");
    download_prefix_ = Config::instance().getString("download_prefix", "/download/");
    if (!storage_dir_.empty() && storage_dir_.back() != '/') {
        storage_dir_ += '/';
    }

    batch_size_        = static_cast<size_t>(Config::instance().getInt("db_batch_size", 100));
    batch_interval_ms_ = Config::instance().getInt("db_batch_interval_ms", 200);
    max_deadlock_retry_ = Config::instance().getInt("db_max_deadlock_retry", 5);
    if (batch_size_ == 0) batch_size_ = 1;

    // flush 线程数：默认对齐连接池大小（让每个 flush 线程都能借到自己的连接），
    // 也可由配置显式指定。上限收敛到连接池大小——线程数多于连接数没有意义，
    // 多出的线程只会在 acquire() 处排队等连接。
    flush_threads_num_ = static_cast<size_t>(
        Config::instance().getInt("db_flush_threads", pool_size));
    if (flush_threads_num_ == 0) flush_threads_num_ = 1;
    if (flush_threads_num_ > static_cast<size_t>(pool_size)) {
        flush_threads_num_ = static_cast<size_t>(pool_size);
    }

    // 2. 建立落盘根目录
    std::error_code ec;
    fs::create_directories(storage_dir_, ec);
    if (ec) {
        LOG_ERROR("Failed to create storage dir " + storage_dir_ + ": " + ec.message());
        return false;
    }

    // 3. 建立连接池
    // MySQLPool::init 形参为 (host, user, password, database, port, pool_size)
    if (!pool_.init(host.c_str(), user.c_str(), pass.c_str(), db.c_str(),
                    static_cast<unsigned int>(port),
                    static_cast<size_t>(pool_size))) {
        LOG_ERROR("MySQL pool init failed");
        return false;
    }

    // 4. 启动一组后台 flush 工作线程
    // 必须先把 running_ 置 true 再建线程，避免线程启动后立刻看到 false 而退出。
    running_ = true;
    ready_ = true;
    flush_threads_.reserve(flush_threads_num_);
    for (size_t i = 0; i < flush_threads_num_; ++i) {
        flush_threads_.emplace_back(&StorageManager::flushLoop, this);
    }

    LOG_INFO("StorageManager ready (db=" + db + ", pool=" + std::to_string(pool_size) +
             ", flush_threads=" + std::to_string(flush_threads_num_) +
             ", batch=" + std::to_string(batch_size_) + "/" +
             std::to_string(batch_interval_ms_) + "ms, dir=" + storage_dir_ + ")");
    return true;
}

void StorageManager::shutdown() {
    // 幂等：已停则直接返回（shutdown 可能被主程序和析构重复调用）
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;
    }
    ready_ = false;

    // 唤醒所有 flush 线程：它们醒来后会先把 pending_ 里的剩余批次冲刷干净，
    // 再因 running_==false 且队列空而退出。notify_all 确保每个线程都被叫醒。
    queue_cv_.notify_all();
    for (auto& t : flush_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    flush_threads_.clear();

    // 全部 flush 线程退出后再关连接池，保证关池时没有连接仍被借用
    pool_.shutdown();
}

bool StorageManager::store(const std::string& device_id, const std::string& log_data) {
    if (!ready_) {
        return false;
    }

    // ============ 阶段一：落盘（同步、实时）============
    // 日志内容必须不丢、不乱序，所以在调用线程里同步写完文件。
    // 这里的 file_mutex_ 只保护文件追加写这段临界区，不涉及任何数据库操作。
    std::string file_path;
    std::int64_t fsize = 0, mtime = 0, atime = 0;
    std::string file_hash;
    {
        std::lock_guard<std::mutex> lock(file_mutex_);

        // 设备子目录：deep_storage/<device_id>/
        std::string dev_dir = storage_dir_ + device_id + "/";
        std::error_code ec;
        fs::create_directories(dev_dir, ec);
        if (ec) {
            LOG_ERROR("Failed to create device dir " + dev_dir + ": " + ec.message());
            return false;
        }

        // 以追加模式写入该设备的日志文件，保证每条日志独占一行
        file_path = dev_dir + device_id + ".log";
        {
            std::ofstream ofs(file_path, std::ios::app | std::ios::binary);
            if (!ofs.is_open()) {
                LOG_ERROR("Failed to open storage file " + file_path);
                return false;
            }
            ofs << log_data;
            if (!log_data.empty() && log_data.back() != '\n') {
                ofs << '\n';
            }
        }

        // 追加写后，采集文件当前的真实元数据（大小/时间）
        struct stat st{};
        if (::stat(file_path.c_str(), &st) != 0) {
            LOG_ERROR("Failed to stat storage file " + file_path);
            return false;
        }
        fsize = static_cast<std::int64_t>(st.st_size);
        mtime = static_cast<std::int64_t>(st.st_mtime);
        atime = static_cast<std::int64_t>(st.st_atime);

        // 读取整文件算内容哈希（作为一致性/去重依据）
        std::string content;
        std::ifstream ifs(file_path, std::ios::binary);
        if (ifs.is_open()) {
            content.assign((std::istreambuf_iterator<char>(ifs)),
                           std::istreambuf_iterator<char>());
        }
        file_hash = hashContent(content);
    }

    // ============ 阶段二：投递元数据（异步、攒批）============
    // 把这条日志对应的文件元数据打包成纯数据结构，放进共享队列后立即返回。
    // 真正的写库由 flush 线程组异步完成，调用线程不阻塞在数据库上。
    FileMeta meta;
    meta.device_id = device_id;
    meta.storage_path = file_path;
    meta.url = download_prefix_ + device_id + "/" + device_id + ".log";
    meta.fsize = fsize;
    meta.mtime = mtime;
    meta.atime = atime;
    meta.file_hash = std::move(file_hash);
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        pending_.push_back(std::move(meta));
    }
    // 每入队一条就唤醒「一个」空闲 flush 线程去处理，使多批数据能被多个线程
    // 并行认领。用 notify_one（而非 notify_all）避免惊群：只需叫醒一个即可，
    // 若队列里还有更多，被唤醒的线程取走一批后其余线程会继续被后续 notify 唤醒。
    queue_cv_.notify_one();
    return true;
}

void StorageManager::flushLoop() {
    // 本函数会被多个 flush 线程同时执行。它们竞争同一个 pending_ 队列，
    // 各自取走一批、各自借连接写库，从而并行使用连接池里的多条连接。
    while (true) {
        std::vector<FileMeta> batch;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // 等待被唤醒，唤醒条件满足其一即返回：
            //   队列已攒够一批（pending_.size() >= batch_size_）：达到批量阈值；
            //   已停机（!running_）：需要收尾冲刷并退出。
            // 另外还带一个 batch_interval_ms_ 超时：即使没攒够一批，超时后也醒来，
            // 把已有的零散数据取走写库，避免低负载时数据长时间滞留在内存。
            // 多消费者场景下每个线程各自持有这把锁的时间很短（只做"取一批"），
            // 取完立刻解锁去写库，因此彼此几乎不互相阻塞。
            queue_cv_.wait_for(lock,
                std::chrono::milliseconds(batch_interval_ms_),
                [this] { return pending_.size() >= batch_size_ || !running_; });

            if (pending_.empty()) {
                // 队列空：已停机则退出线程；否则是超时空转，继续下一轮等待。
                if (!running_) break;
                continue;
            }

            // 从队头取走最多 batch_size_ 条，组成本线程要落库的一批。
            // 取的过程仍持锁，保证不同 flush 线程不会取到重复数据。
            size_t take = std::min(pending_.size(), batch_size_);
            batch.reserve(take);
            for (size_t i = 0; i < take; ++i) {
                batch.push_back(std::move(pending_.front()));
                pending_.pop_front();
            }
        }   // 解锁：接下来的写库不再占用队列锁，其他 flush 线程可继续取批

        // 写库在锁外进行：借到的连接由本线程独占，与其他线程的连接互不干扰。
        if (!flushBatch(batch)) {
            LOG_ERROR("flushBatch failed, " + std::to_string(batch.size()) +
                      " metadata records dropped");
        }
    }
}

bool StorageManager::flushBatch(std::vector<FileMeta>& batch) {
    if (batch.empty()) {
        return true;
    }

    // 从连接池借一条连接，用 RAII 守卫保证无论正常返回还是中途 return，
    // 离开作用域时都会自动归还连接。本线程在此期间独占这条连接。
    PooledConn c(pool_);
    if (!c) {
        return false;   // 池已关闭（shutdown 期间），本批放弃
    }

    // 降低死锁概率：批内按 url 排序，使所有 flush 线程都以一致的顺序访问
    // files 表的行。死锁的根因是不同事务以相反顺序请求同一组行的锁，统一顺序
    // 能从源头减少循环等待。（这只是降低概率，仍需下面的重试兜底。）
    std::sort(batch.begin(), batch.end(),
              [](const FileMeta& a, const FileMeta& b) { return a.url < b.url; });

    // 预拼三条 SQL
    // 1) 批量 upsert files：整批合并成一条多值 INSERT，一次往返落库
    std::ostringstream fsql;
    fsql << "INSERT INTO files (mtime, atime, fsize, storage_path, url, file_hash, device_id, storage_type) VALUES ";
    for (size_t i = 0; i < batch.size(); ++i) {
        const FileMeta& m = batch[i];
        if (i) fsql << ",";
        // 字符串字段经 escape 转义防注入；数值字段直接拼
        fsql << "(" << m.mtime << "," << m.atime << "," << m.fsize
             << ",'" << c->escape(m.storage_path) << "'"
             << ",'" << c->escape(m.url) << "'"
             << ",'" << c->escape(m.file_hash) << "'"
             << ",'" << c->escape(m.device_id) << "',0)";
    }
    // url 是唯一键：首次插入，重复则更新为最新大小/时间/哈希
    fsql << " ON DUPLICATE KEY UPDATE "
            "mtime=VALUES(mtime), atime=VALUES(atime), "
            "fsize=VALUES(fsize), file_hash=VALUES(file_hash)";
    const std::string files_sql = fsql.str();

    // 2) 批量 upsert devices：本批设备去重后拼成一条多值 INSERT
    std::unordered_set<std::string> devset;
    for (const auto& m : batch) devset.insert(m.device_id);
    // 转成有序 vector，让 devices/file_count 的加锁顺序也稳定，进一步降死锁概率
    std::vector<std::string> devices(devset.begin(), devset.end());
    std::sort(devices.begin(), devices.end());

    std::ostringstream dsql, in_list;
    dsql << "INSERT INTO devices (device_id, last_active) VALUES ";
    bool first = true;
    for (const auto& d : devices) {
        std::string ed = c->escape(d);
        if (!first) { dsql << ","; in_list << ","; }
        first = false;
        dsql << "('" << ed << "', CURRENT_TIMESTAMP)";
        in_list << "'" << ed << "'";
    }
    dsql << " ON DUPLICATE KEY UPDATE last_active=CURRENT_TIMESTAMP";
    const std::string devices_sql = dsql.str();

    // 3) 一条 UPDATE ... JOIN 聚合更新本批设备的 file_count
    const std::string count_sql =
        "UPDATE devices dv "
        "JOIN (SELECT device_id, COUNT(*) AS cnt FROM files "
        "      WHERE device_id IN (" + in_list.str() + ") GROUP BY device_id) f "
        "  ON dv.device_id = f.device_id "
        "SET dv.file_count = f.cnt";

    // 把三条 SQL 放进一个事务，并对 InnoDB 死锁做回滚 + 退避重试
    for (int attempt = 0; attempt <= max_deadlock_retry_; ++attempt) {
        if (!c->execute("START TRANSACTION")) {
            LOG_ERROR("START TRANSACTION failed: " + c->lastError());
            return false;
        }

        bool ok = c->execute(files_sql.c_str())
               && c->execute(devices_sql.c_str())
               && c->execute(count_sql.c_str());

        if (ok) {
            if (c->execute("COMMIT")) {
                return true;   // 整批成功落库
            }
            // COMMIT 也可能因死锁失败，落入下面的重试判断
        }

        // 走到这里说明本次事务失败：先记录错误码，回滚，再决定是否重试。
        unsigned int err = c->lastErrno();
        c->execute("ROLLBACK");

        // 1213=死锁，1205=锁等待超时：可重试的暂时性错误。其他错误直接放弃。
        bool retryable = (err == 1213 || err == 1205);
        if (!retryable || attempt == max_deadlock_retry_) {
            LOG_ERROR("flushBatch giving up after " + std::to_string(attempt + 1) +
                      " attempt(s), errno=" + std::to_string(err) +
                      ", " + std::to_string(batch.size()) + " records dropped");
            return false;
        }
        // 指数退避 + 线程相关抖动，错开各线程的重试时刻，避免再次撞死锁。
        int backoff_ms = (1 << attempt) * 2;   // 2,4,8,16,32 ms
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    }
    return false;
}

std::string StorageManager::hashContent(const std::string& content) const {
    // 轻量内容哈希：FNV-1a 64 位，取 16 个十六进制字符，落在 file_hash(32) 内。
    // 目的是一致性校验/去重线索，非加密强度。
    std::uint64_t h = 1469598103934665603ULL;      // FNV offset basis
    const std::uint64_t prime = 1099511628211ULL;  // FNV prime
    for (unsigned char ch : content) {
        h ^= ch;
        h *= prime;
    }
    char buf[17];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(h));
    return std::string(buf);
}
