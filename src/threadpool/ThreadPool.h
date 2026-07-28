#pragma once

#include <queue>
#include <vector>
#include <thread>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>

class ThreadPool {
public:
    using task_t = std::function<void()>;

    // 构造函数，线程数量默认使用 CPU 核心数
    explicit ThreadPool(size_t thread_count = std::thread::hardware_concurrency());
    ~ThreadPool();

    // 将任务添加到任务队列，并唤醒一个线程
    void submit(task_t task);
    // 关闭线程池
    void shutdown();

    // 获取工作线程数量
    size_t getThreadCount() const {
        return workers_.size();
    }
private:
    // 工作线程主循环
    void workerThread();

    std::vector<std::thread> workers_;
    std::queue<task_t> tasks_;

    std::mutex mutex_;
    std::condition_variable cv_;

    bool running_{ false };
};