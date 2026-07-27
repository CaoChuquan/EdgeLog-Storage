#include <iostream>
#include "AsyncLogger.h"
#include "Config.h"

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
    LOG_INFO("Running...");

    // 关闭日志
    AsyncLogger::instance().shutdown();

    return 0;
}