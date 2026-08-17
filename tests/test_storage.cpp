// 存储层端到端回归测试
//   在同一进程内：初始化 StorageManager（连 MySQL、建目录）->
//   为一个临时设备写入两条日志 -> 校验文件落盘内容 + files/devices 表记录 ->
//   清理该测试设备的库记录与文件。
// 用于回归验证「日志 -> 落盘文件 + 元数据入库」这条存储链路的正确性。
#include "AsyncLogger.h"
#include "Config.h"
#include "StorageManager.h"
#include "MySQLConn.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;

int main() {
    AsyncLogger::instance().init("./logs_test", LogLevel::DEBUG);
    Config::instance().load("./config.json");

    if (!StorageManager::instance().init()) {
        std::cerr << "FAILED: storage init failed (MySQL up? config correct?)\n";
        return 1;
    }

    // 用带时间戳的临时设备 id，避免污染真实数据、保证可重复运行
    auto now = std::chrono::system_clock::now().time_since_epoch();
    long ts = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    std::string device_id = "test_dev_" + std::to_string(ts);

    // 存两条日志
    bool ok1 = StorageManager::instance().store(device_id, "log_test_1");
    bool ok2 = StorageManager::instance().store(device_id, "log_test_2");
    if (!ok1 || !ok2) {
        std::cerr << "FAILED: store() returned false\n";
        StorageManager::instance().shutdown();
        return 2;
    }

    // 元数据是异步攒批写库的：shutdown 会 join flush 线程并冲刷剩余批次，
    // 之后库里数据才稳定，可安全校验。
    StorageManager::instance().shutdown();

    // 校验 1：文件落盘且含两行
    std::string dir = Config::instance().getString("deep_storage_dir", "./deep_storage/");
    if (!dir.empty() && dir.back() != '/') dir += '/';
    std::string file_path = dir + device_id + "/" + device_id + ".log";

    std::ifstream ifs(file_path);
    if (!ifs.is_open()) {
        std::cerr << "FAILED: storage file not found: " << file_path << "\n";
        return 3;
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(ifs, line)) lines.push_back(line);
    ifs.close();
    if (lines.size() != 2 || lines[0] != "log_test_1" || lines[1] != "log_test_2") {
        std::cerr << "FAILED: file content mismatch, got " << lines.size() << " lines\n";
        return 4;
    }

    // 校验 2：数据库记录（直接开一条连接查）
    // MySQLConn::connect 形参为 (host, user, password, database, port)
    MySQLConn conn;
    if (!conn.connect(
            Config::instance().getString("mysql_host", "127.0.0.1").c_str(),
            Config::instance().getString("mysql_user", "root").c_str(),
            Config::instance().getString("mysql_password", "").c_str(),
            Config::instance().getString("mysql_database", "edgelog").c_str(),
            static_cast<unsigned int>(Config::instance().getInt("mysql_port", 3306)))) {
        std::cerr << "FAILED: verify-connect failed: " << conn.lastError() << "\n";
        return 5;
    }
    std::string dev = conn.escape(device_id);

    // files 表：应恰好一条记录，且 fsize 与文件实际大小一致
    std::vector<std::vector<std::string>> rows;
    std::string q_files =
        "SELECT storage_path, fsize, url, device_id FROM files WHERE device_id='" + dev + "'";
    if (!conn.query(q_files.c_str(), rows)) {
        std::cerr << "FAILED: query files: " << conn.lastError() << "\n";
        return 6;
    }
    if (rows.size() != 1) {
        std::cerr << "FAILED: expected 1 files row, got " << rows.size() << "\n";
        return 7;
    }
    long db_fsize = std::stol(rows[0][1]);
    long real_fsize = static_cast<long>(fs::file_size(file_path));
    if (db_fsize != real_fsize) {
        std::cerr << "FAILED: fsize mismatch db=" << db_fsize << " real=" << real_fsize << "\n";
        return 8;
    }
    std::cout << "files row: path=" << rows[0][0] << " fsize=" << rows[0][1]
              << " url=" << rows[0][2] << "\n";

    // devices 表：应有该设备，且 file_count=1
    std::vector<std::vector<std::string>> drows;
    std::string q_dev =
        "SELECT device_id, file_count FROM devices WHERE device_id='" + dev + "'";
    if (!conn.query(q_dev.c_str(), drows)) {
        std::cerr << "FAILED: query devices: " << conn.lastError() << "\n";
        return 9;
    }
    if (drows.size() != 1 || std::stoi(drows[0][1]) != 1) {
        std::cerr << "FAILED: devices row/file_count wrong\n";
        return 10;
    }
    std::cout << "devices row: id=" << drows[0][0] << " file_count=" << drows[0][1] << "\n";

    // 清理测试数据
    std::string del_files = "DELETE FROM files WHERE device_id='" + dev + "'";
    std::string del_dev   = "DELETE FROM devices WHERE device_id='" + dev + "'";
    conn.execute(del_files.c_str());
    conn.execute(del_dev.c_str());
    std::error_code ec;
    fs::remove_all(dir + device_id, ec);

    AsyncLogger::instance().shutdown();

    std::cout << "PASSED\n";
    return 0;
}