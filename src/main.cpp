#include <iostream>
#include <csignal>
#include "AsyncLogger.h"
#include "Config.h"
#include "Server.h"

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

    // 注册信号处理，使用 ctrl + c / kill 停止
    signal(SIGINT, handleSignal);
    signal(SIGTERM, handleSignal);

    // 启动网络服务器
    int port = Config::instance().getInt("port", 8080);

    // 关闭日志
    AsyncLogger::instance().shutdown();

    return 0;
}