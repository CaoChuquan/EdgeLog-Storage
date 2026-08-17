// 存储层并发/批量压测
//   起 N 个线程并发 store 大量日志（模拟多设备高频上报），验证：
//     1. 连接池 + 攒批写库下，全部元数据最终正确落库（每设备一条 files 记录）；
//     2. devices 表 file_count 正确；
//   并打印吞吐，作为"连接池 + 批量写入"优化效果的量化依据。
#include "AsyncLogger.h"
#include "Config.h"
#include "StorageManager.h"
#include "MySQLConn.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;

int main() {
    AsyncLogger::instance().init("./logs_bench", LogLevel::WARN);
    Config::instance().load("./config.json");

    if (!StorageManager::instance().init()) {
        std::cerr << "FAILED: storage init failed\n";
        return 1;
    }

    const int kThreads = 8;           // 并发线程数
    const int kDevPerThread = 25;     // 每线程负责的设备数
    const int kLogsPerDevice = 20;    // 每设备发送的日志条数
    const int kTotalDevices = kThreads * kDevPerThread;
    const long kTotalLogs = static_cast<long>(kTotalDevices) * kLogsPerDevice;

    auto now = std::chrono::system_clock::now().time_since_epoch();
    long ts = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    std::string prefix = "bench_" + std::to_string(ts) + "_";

    std::atomic<long> ok{ 0 };
    auto t0 = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            for (int d = 0; d < kDevPerThread; ++d) {
                std::string dev = prefix + std::to_string(t) + "_" + std::to_string(d);
                for (int i = 0; i < kLogsPerDevice; ++i) {
                    if (StorageManager::instance().store(dev, "val=" + std::to_string(i))) {
                        ok.fetch_add(1);
                    }
                }
            }
        });
    }
    for (auto& w : workers) w.join();

    auto t1 = std::chrono::steady_clock::now();
    double store_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // shutdown 会 join flush 线程、冲刷剩余批次，确保元数据全部落库
    StorageManager::instance().shutdown();
    auto t2 = std::chrono::steady_clock::now();
    double flush_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();

    std::cout << "stored " << ok.load() << "/" << kTotalLogs << " logs from "
              << kThreads << " threads\n";
    std::cout << "store phase: " << store_ms << " ms ("
              << (kTotalLogs / (store_ms / 1000.0)) << " logs/s into pipeline)\n";
    std::cout << "flush drain: " << flush_ms << " ms\n";

    if (ok.load() != kTotalLogs) {
        std::cerr << "FAILED: some store() calls failed\n";
        return 2;
    }

    // 校验：files 表应恰好有 kTotalDevices 条（每设备一条）
    // MySQLConn::connect 形参为 (host, user, password, database, port)
    MySQLConn conn;
    if (!conn.connect(
            Config::instance().getString("mysql_host", "127.0.0.1").c_str(),
            Config::instance().getString("mysql_user", "root").c_str(),
            Config::instance().getString("mysql_password", "").c_str(),
            Config::instance().getString("mysql_database", "edgelog").c_str(),
            static_cast<unsigned int>(Config::instance().getInt("mysql_port", 3306)))) {
        std::cerr << "FAILED: verify-connect failed: " << conn.lastError() << "\n";
        return 3;
    }
    std::string like = conn.escape(prefix) + "%";

    std::vector<std::vector<std::string>> rows;
    std::string q_files = "SELECT COUNT(*) FROM files WHERE device_id LIKE '" + like + "'";
    conn.query(q_files.c_str(), rows);
    long files_cnt = rows.empty() ? -1 : std::stol(rows[0][0]);

    std::string q_dev = "SELECT COUNT(*) FROM devices WHERE device_id LIKE '" + like + "'";
    conn.query(q_dev.c_str(), rows);
    long dev_cnt = rows.empty() ? -1 : std::stol(rows[0][0]);

    // 每设备一文件，file_count 应全为 1
    std::string q_dev_ok =
        "SELECT COUNT(*) FROM devices WHERE device_id LIKE '" + like + "' AND file_count=1";
    conn.query(q_dev_ok.c_str(), rows);
    long dev_cnt_ok = rows.empty() ? -1 : std::stol(rows[0][0]);

    std::cout << "files rows=" << files_cnt << " (expect " << kTotalDevices << "), "
              << "devices rows=" << dev_cnt << ", file_count=1 rows=" << dev_cnt_ok << "\n";

    // 清理
    std::string del_files = "DELETE FROM files WHERE device_id LIKE '" + like + "'";
    std::string del_dev   = "DELETE FROM devices WHERE device_id LIKE '" + like + "'";
    conn.execute(del_files.c_str());
    conn.execute(del_dev.c_str());
    std::string dir = Config::instance().getString("deep_storage_dir", "./deep_storage/");
    if (!dir.empty() && dir.back() != '/') dir += '/';
    std::error_code ec;
    for (int t = 0; t < kThreads; ++t)
        for (int d = 0; d < kDevPerThread; ++d)
            fs::remove_all(dir + prefix + std::to_string(t) + "_" + std::to_string(d), ec);

    AsyncLogger::instance().shutdown();

    if (files_cnt != kTotalDevices || dev_cnt != kTotalDevices || dev_cnt_ok != kTotalDevices) {
        std::cerr << "FAILED: db row/count mismatch\n";
        return 4;
    }
    std::cout << "PASSED\n";
    return 0;
}