#include "ThreadPool.h"
#include "AsyncLogger.h"

ThreadPool::ThreadPool(size_t thread_count) {
    // 如果 CPU 核心数获取失败则默认线程数量为 4
    if (thread_count == 0) {
        thread_count = 4;
    }
    LOG_INFO("Creating thread pool with " 
        + std::to_string(thread_count) + " threads");
    
    // 创建工作线程
    running_ = true;
    for (int i = 0; i < thread_count; ++i) {
        workers_.emplace_back(std::thread(&ThreadPool::workerThread, this));
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::submit(task_t task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            LOG_WARN("Cannot submit task, thread pool is not running");
            return;
        }
        // 将任务入队
        tasks_.push(std::move(task));
    }
    // 唤醒一个线程执行任务
    cv_.notify_one();
}

void ThreadPool::workerThread() {
    /*
    1. 线程池运行，队列里有任务 --> 取出任务再执行，工作线程继续运行
    2. 线程池关闭，队列里有任务 --> 取出任务再执行，关闭工作线程
    3. 线程池运行，队列里没任务 --> 阻塞
    4. 线程池关闭，队列里没任务 --> 直接关闭工作线程
    */
    while (true) {
        task_t task;

        // 获取互斥量并等待条件变量
        {
            std::unique_lock<std::mutex> lock(mutex_);
            // 队列为空且线程池在运行时等待
            cv_.wait(lock, [this]() {
                // 等待条件：队列不为空或者线程池停止运行
                return !running_ || !(this->tasks_.empty());
            });

            // 线程池关闭，队列里没任务 --> 直接关闭工作线程
            if (!running_ && tasks_.empty()) {
                return;
            }

            // 从队列中取出任务           
            task = std::move(tasks_.front());
            tasks_.pop();
        }

        // 在无锁状态下执行任务，不阻塞队列
        try {
            task();
        }
        catch (const std::exception& e) {
            LOG_ERROR("Task execution failed: " + std::string(e.what()));
        }
        catch (...) {
            LOG_ERROR("Task execution failed with unknown exception");
        }
    }
}

void ThreadPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return; // 防止重复关闭
        }
        running_ = false;
    }

    // 唤醒所有在等待的工作线程
    cv_.notify_all();
    // 等待工作线程
    for (std::thread& t : workers_) {
        t.join();
    }

    LOG_INFO("Thread pool shutdown completed");
}