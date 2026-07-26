#pragma once

#include "Level.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <thread>
#include <atomic>

constexpr size_t MAXFILESIZE = 10 * 1024 * 1024;
constexpr size_t MAXQUEUECAPCITY = 1000;
constexpr int FLUSHBATCHSIZE = 100;

// 日志消息结构
struct LogMessage {
    LogLevel level;
    std::string message;
    std::string timestamp;
};


// 全局唯一的异步日志类
/*
本模块实现了一个高性能的异步日志系统，采用生产者-消费者模式：
    生产者（业务线程）：调用 Log() 方法将日志消息放入队列后立即返回
    消费者（工作线程）：独立线程从队列中取出消息并写入文件

核心特性：
    1. 异步写入：日志写入不阻塞业务线程，提高系统吞吐量
    2. 日志级别过滤：可配置只输出指定级别及以上的日志
    3. 文件滚动：当日志文件达到指定大小（默认10MB）时自动切换新文件
    4. 单例模式：全局唯一实例，方便在项目中统一使用
 */
class AsyncLogger {
public:
    ~AsyncLogger();

    void init(const std::string& log_dir, LogLevel level = LogLevel::INFO);
    void shutdown();

    // 写日志
    void log(LogLevel level, const std::string& message);

    // 获取唯一实例
    static AsyncLogger& instance() {
        static AsyncLogger ins;
        return ins;
    }
private:
    AsyncLogger() {}
    // 工作线程，持续等待队列中的消息，去除消息后写入文件
    void workerThread();
    // 获取当前时间戳
    std::string getCurrentTime();
    // 关闭当日志文件，创建新的文件
    void rotateFile();
    // 生成新文件名
    std::string getNewFileName();

    // 队列相关成员
    std::queue<LogMessage> message_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;

    // 线程相关成员
    std::thread worker_thread_;
    std::atomic<bool> running_{ false };

    // 文件相关成员
    std::ofstream log_file_;
    std::string log_dir_;
    std::string current_filename_; // 当前日志文件名
    LogLevel level_{ LogLevel::INFO }; // 日志级别过滤阈值
    size_t file_size_{ 0 }; // 当前文件大小
    int flush_count_{ 0 };
};


// 日志宏定义，简化调用操作
#define LOG_DEBUG(msg) AsyncLogger::instance().log(LogLevel::DEBUG, msg)
#define LOG_INFO(msg) AsyncLogger::instance().log(LogLevel::INFO, msg)
#define LOG_WARN(msg) AsyncLogger::instance().log(LogLevel::WARN, msg)
#define LOG_ERROR(msg) AsyncLogger::instance().log(LogLevel::ERROR, msg)
#define LOG_FATAL(msg) AsyncLogger::instance().log(LogLevel::FATAL, msg)