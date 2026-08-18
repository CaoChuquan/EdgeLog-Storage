#pragma once

#include "ThreadPool.h"
#include <unistd.h>
#include <string>
#include <functional>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <memory>

// 存储客户端连接的所有状态信息，包括 socket、地址和接收缓冲区
struct ClientConnection {
    int sockfd;
    std::string addr;
    char buffer[4096];
    size_t buffer_pos;

    ClientConnection(int fd, const std::string& address)
        : sockfd(fd), addr(address), buffer_pos(0) {}
    ~ClientConnection() {
        if (sockfd >= 0) ::close(sockfd);
    }
};

class NetworkServer {
public:
    // 日志处理回调：上层拿到 (设备号, seq, 日志内容) 后落库
    // 返回 true 表示该条已可靠持久化，网络层随后会给设备回 ACK <seq>
    // 返回 false 表示落库失败，不回 ACK，设备端超时后会重传
    using LogDataCallback = std::function<bool(std::string, uint64_t, std::string)>;

    NetworkServer() {}
    ~NetworkServer() { shutdown(); }

    static NetworkServer& instance() {
        static NetworkServer server;
        return server;
    }

    // 初始化网络服务器
    bool init(int port = 8080, size_t max_connections = 1000);
    // 停止监听线程，关闭所有客户端连接，释放所有资源
    void shutdown();
    // 当收到边缘设备发出的日志时，通过此回调通知上层
    void setLogDataCallback(LogDataCallback cb) {
        cb_ = cb;
    }
private:
    void eventLoop();

    bool setNonBlocking(int sockfd);
    bool addEpollEvent(int sockfd, uint32_t events);

    // 处理新连接
    void handleAccept();
    // 处理读事件
    void handleRead(int client_fd);
    // 处理连接断开事件
    void handleDisconnect(int client_fd);

    // 解析并处理一行完整日志：拆出 seq 与 payload，投递给上层，成功后回 ACK。
    void handleLine(ClientConnection* conn, char* line_begin, size_t line_len);

    int listenfd_{ -1 };
    int epollfd_{ -1 };
    int wakeupfd_{ -1 }; // 用于关闭网络服务器时唤醒阻塞的 epoll_wait()
    int port_{ 8080 };
    size_t max_connections_{ 1000 };

    std::thread event_thread_;
    std::atomic<bool> running_{ false };
    std::unordered_map<int, std::unique_ptr<ClientConnection>> clients_;

    LogDataCallback cb_{ nullptr };

    std::unique_ptr<ThreadPool> thread_pool_;
};