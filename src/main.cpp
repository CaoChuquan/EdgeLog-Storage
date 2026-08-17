#include <iostream>
#include <csignal>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include "AsyncLogger.h"
#include "Config.h"
#include "Server.h"
#include "StorageManager.h"

// 全局停机标志，由信号处理函数置位，主线程等待它
namespace {
    std::atomic<bool> g_running{ true };
    std::condition_variable g_cv;
    std::mutex g_mutex;

    void handleSignal(int sig) {
        (void)sig;
        g_running = false;
        g_cv.notify_one();
    }
}

int main() {
    using namespace std;

    // 加载配置文件
    Config::instance().load("./config.json");
    // 从配置文件中取得数据
    string log_dir = Config::instance().getString("log_dir");
    string log_level_str = Config::instance().getString("log_level");
    LogLevel level = stringToLevel(log_level_str);

    // 使用配置文件中的数据初始化日志
    AsyncLogger::instance().init(log_dir, level);
    // 输出服务启动日志（包含配置信息）
    string service_name = Config::instance().getString("service_name");
    string service_version = Config::instance().getString("service_version");
    LOG_INFO(service_name + " v" + service_version);
    LOG_INFO("Log directory: " + log_dir);
    LOG_INFO("Log level: " + log_level_str);
    LOG_DEBUG("Service starting...");

    // 注册信号处理，支持 Ctrl-C / kill 优雅停机
    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);

    // 初始化存储层：连接 MySQL、准备落盘目录。失败则退出。
    if (!StorageManager::instance().init()) {
        LOG_FATAL("Failed to init storage layer, exiting");
        AsyncLogger::instance().shutdown();
        return 1;
    }

    // 启动网络服务器：收到设备日志后，落盘到存储层并转写进运行日志
    int port = Config::instance().getInt("port", 8080);
    NetworkServer::instance().setLogDataCallback(
        [](std::string device_id, std::string log_data) {
            if (!StorageManager::instance().store(device_id, log_data)) {
                LOG_ERROR("Failed to store log from device " + device_id);
            }
            LOG_INFO("[Device:" + device_id + "] " + log_data);
        });

    if (!NetworkServer::instance().init(port)) {
        LOG_FATAL("Failed to start network server, exiting");
        AsyncLogger::instance().shutdown();
        return 1;
    }

    LOG_INFO("Running... (press Ctrl-C to stop)");

    // 主线程挂起，等待停机信号。
    // 若配置了 run_seconds（>0），则最多运行该秒数后自动停机（便于自动化测试）。
    int run_seconds = Config::instance().getInt("run_seconds", 0);
    {
        std::unique_lock<std::mutex> lock(g_mutex);
        if (run_seconds > 0) {
            g_cv.wait_for(lock, std::chrono::seconds(run_seconds),
                          [] { return !g_running.load(); });
        } else {
            g_cv.wait(lock, [] { return !g_running.load(); });
        }
    }

    // 优雅停机：先停服务器（不再接收新数据、线程池排空），
    // 确保没有回调再访问存储层后，再关存储层，最后关日志（冲刷队列）
    LOG_INFO("Shutting down...");
    NetworkServer::instance().shutdown();
    StorageManager::instance().shutdown();
    AsyncLogger::instance().shutdown();

    return 0;
}