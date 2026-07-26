#include <iostream>
#include "AsyncLogger.h"

int main() {
    using namespace std;

    // 初始化异步日志
    AsyncLogger::instance().init("./logs", LogLevel::DEBUG);
    
    // 输出服务启动日志
    LOG_INFO("EdgeLog Storage Service v1.0.0");   // 服务版本信息
    LOG_DEBUG("Service starting...");            // 调试信息（开发阶段使用）
    LOG_INFO("Running...");                      // 服务运行状态

    cout << "Hello !" << endl;

    return 0;
}