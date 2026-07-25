#include "AsyncLogger.h"
#include <chrono>
#include <ctime>
#include <sstream>
#include <iomanip>

using std::string;

constexpr size_t MAXFILESIZE = 10 * 1024 * 1024;

// // 生成新文件名
// static const char* getNewFileName() {
//     // 格式：edgelog_YYYYMMDD_HHMMSS.log
//     auto now = std::chrono::system_clock::now();
//     auto time_t = std::chrono::system_clock::to_time_t(now);
    
//     std::tm tm;
//     localtime_r(&time_t, &tm);
    
//     std::ostringstream oss;
//     oss << log_dir_ << "/edgelog_"
//         << std::setfill('0')
//         << std::setw(4) << (tm.tm_year + 1900)
//         << std::setw(2) << (tm.tm_mon + 1)
//         << std::setw(2) << tm.tm_mday << "_"
//         << std::setw(2) << tm.tm_hour
//         << std::setw(2) << tm.tm_min
//         << std::setw(2) << tm.tm_sec << ".log";
    
//     return oss.str();
// }

// 确保程序退出时日志系统被正确关闭，需等待工作线程
// 完成剩余的日志的写入
AsyncLogger::~AsyncLogger() {
    if (running_) {
        running_ = false;

        // 唤醒工作线程，确保剩余日志写入完毕
        cv_.notify_one();

        // 等待工作线程完成
        if (worker_thread_.joinable()) worker_thread_.join();

        // 关闭日志文件
        if (log_file_.is_open()) log_file_.close();
    }
}

// 初始化日志
void AsyncLogger::init(const string& log_dir, LogLevel level) {
    // 防止多次调用 init()
    if (running_.load()) return; // 已经运行则直接返回

    log_dir_ = log_dir;
    level_ = level;

    // 第一次打开日志文件
    rotateFile();

    // 设置运行状态并启动工作线程
    running_ = true;
    worker_thread_ = std::thread(&AsyncLogger::workerThread, this);
}

void AsyncLogger::log(LogLevel level, const string& message) {
    // 级别过滤
    if (level < level_) return;

    LogMessage msg;
    msg.level = level;
    msg.message = message;
    msg.timestamp = getCurrentTime();

    // 加入队列，加锁保护
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        message_queue_.push(std::move(msg));
    }

    // 通知工作线程有日志消息
    cv_.notify_one();
}

void AsyncLogger::workerThread() {
    while (running_) {
        // 加锁并请求等待条件
        std::unique_lock<std::mutex> lock(queue_mutex_);
        cv_.wait(lock, [this]() {
            // 防止虚假唤醒
            return !(this->message_queue_.empty()) || !running_;
        });

        // 处理队列中的消息
        while (!message_queue_.empty()) {
            // 取出消息
            LogMessage msg = std::move(message_queue_.front());
            message_queue_.pop();

            // 临时释放锁，允许向队列中加入消息
            lock.unlock();

            // 构建日志
            string log = "[" + msg.timestamp + "] [" + LeveltoString(msg.level) + "] " + msg.message + "\n";

            // 确保文件已经打开
            if (!log_file_.is_open()) rotateFile();

            // 写入文件并且刷新
            log_file_ << log;
            log_file_.flush();
            // 更新文件大小
            file_size_ += log.size();
            // 检查是否需要创建新文件
            if (file_size_ >= MAXFILESIZE) {
                rotateFile();
            }

            // 重新获取锁，继续处理下一条消息
            lock.lock();
        }
    }
}

string AsyncLogger::getCurrentTime() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    
    std::tm tm;
    localtime_r(&time_t, &tm);
    
    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(4) << (tm.tm_year + 1900) << "-"
        << std::setw(2) << (tm.tm_mon + 1) << "-"
        << std::setw(2) << tm.tm_mday << " "
        << std::setw(2) << tm.tm_hour << ":"
        << std::setw(2) << tm.tm_min << ":"
        << std::setw(2) << tm.tm_sec << "."
        << std::setw(3) << ms.count();
    
    return oss.str();
}

void AsyncLogger::rotateFile() {
    // 关闭当前文件
    if (log_file_.is_open()) {
        log_file_.close();
    }
    
    // 生成新文件名（格式：edgelog_YYYYMMDD_HHMMSS.log）
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    
    std::tm tm;
    localtime_r(&time_t, &tm);
    
    std::ostringstream oss;
    oss << log_dir_ << "/edgelog_"
        << std::setfill('0')
        << std::setw(4) << (tm.tm_year + 1900)
        << std::setw(2) << (tm.tm_mon + 1)
        << std::setw(2) << tm.tm_mday << "_"
        << std::setw(2) << tm.tm_hour
        << std::setw(2) << tm.tm_min
        << std::setw(2) << tm.tm_sec << ".log";
    
    current_filename_ = oss.str();
    
    // 打开新文件
    log_file_.open(current_filename_, std::ios::out | std::ios::app);
    
    // 重置文件大小计数器
    file_size_ = 0;
}