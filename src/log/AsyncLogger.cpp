#include "AsyncLogger.h"
#include <chrono>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <filesystem>

using std::string;

// 确保程序退出时日志系统被正确关闭，需等待工作线程
// 完成剩余的日志的写入
AsyncLogger::~AsyncLogger() {
    if (running_) {
        shutdown();
    }
}

void AsyncLogger::shutdown() {
    if (!running_.load()) return;

    running_ = false;
    cv_.notify_one();

    if (worker_thread_.joinable()) worker_thread_.join();

    if (log_file_.is_open()) {
        log_file_.flush();
        log_file_.close();
    }
}

void AsyncLogger::init(const string& log_dir, LogLevel level) {
    if (running_.load()) return;

    log_dir_ = log_dir;
    level_ = level;

    std::filesystem::create_directories(log_dir_);

    rotateFile();

    running_ = true;
    worker_thread_ = std::thread(&AsyncLogger::workerThread, this);
}

void AsyncLogger::log(LogLevel level, const string& message) {
    if (level < level_) return;

    LogMessage msg{ level, message, getCurrentTime() };

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (message_queue_.size() >= MAXQUEUECAPCITY) {
            return;
        }
        message_queue_.push(std::move(msg));
    }

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

            // 写入文件并增加记数
            log_file_ << log;
            ++flush_count_;
            // 日志写入达到一定条数后在刷新
            if (flush_count_ >= FLUSHBATCHSIZE) {
                log_file_.flush();
                flush_count_ = 0;
            }

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

// 生成新文件名
string AsyncLogger::getNewFileName() {
    // 格式：edgelog_YYYYMMDD_HHMMSS.log
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
    
    return oss.str();
}

void AsyncLogger::rotateFile() {
    if (log_file_.is_open()) {
        log_file_.close();
    }

    current_filename_ = getNewFileName();

    log_file_.open(current_filename_, std::ios::out | std::ios::app);
    if (!log_file_.is_open()) {
        return;
    }
    file_size_ = 0;
}