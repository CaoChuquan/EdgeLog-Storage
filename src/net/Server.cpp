#include "Server.h"
#include "AsyncLogger.h"
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <cstring>

using std::string;

bool NetworkServer::init(int port, size_t max_connections) {
    port_ = port;
    max_connections_ = max_connections;

    // 创建监听套接字
    listenfd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd_ < 0) {
        LOG_ERROR("Failed to create socket");
        return false;
    }
    // 设置地址重用
    int opt = 1;
    if (setsockopt(listenfd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        LOG_ERROR("Failed to set SO_REUSEADDR");
        ::close(listenfd_);
        listenfd_ = -1;
        return false;
    }
    // 设置非阻塞模式
    if (!setNonBlocking(listenfd_)) {
        return false;
    }

    // 设置地址
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    addr.sin_addr.s_addr = INADDR_ANY;

    // 绑定地址
    if (bind(listenfd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("Failed to bind socket to port " + std::to_string(port_));
        ::close(listenfd_);
        listenfd_ = -1;
        return false;
    }

    // 开始监听
    if (listen(listenfd_, static_cast<int>(max_connections_)) < 0) {
        LOG_ERROR("Failed to listen on socket");
        ::close(listenfd_);
        listenfd_ = -1;
        return false;
    }

    // 创建 epoll
    epollfd_ = epoll_create1(0);
    if (epollfd_ < 0) {
        LOG_ERROR("Failed to create epoll instance");
        ::close(listenfd_);
        listenfd_ = -1;
        return false;
    }
    // 把 listenfd_ 添加到 epoll
    if (!addEpollEvent(listenfd_, EPOLLIN)) {
        return false;
    }

    // 创建 eventfd 并加入 epoll，shutdown() 时向它写入数据唤醒阻塞的 epoll_wait()
    wakeupfd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeupfd_ < 0) {
        LOG_ERROR("Failed to create eventfd");
        ::close(listenfd_);
        listenfd_ = -1;
        ::close(epollfd_);
        epollfd_ = -1;
        return false;
    }
    if (!addEpollEvent(wakeupfd_, EPOLLIN)) return false;
    

    // 创建线程池，用于业务逻辑
    thread_pool_ = std::make_unique<ThreadPool>();
    // 启动事件循环线程
    running_ = true;
    event_thread_ = std::thread(&NetworkServer::eventLoop, this);

    LOG_INFO("Network server started on port " + std::to_string(port_));
    LOG_INFO("Max connections: " + std::to_string(max_connections_));

    return true;
}

void NetworkServer::shutdown() {
    // 防止重复关闭
    if (running_ == false) return;
    running_ = false;
    
    // 向 eventfd 写入，立即唤醒阻塞在 epoll_wait 上的事件循环线程。
    if (wakeupfd_ >= 0) {
        uint64_t one = 1;
        ssize_t n = ::write(wakeupfd_, &one, sizeof(one));
        (void)n;
    }

    // 先等待事件循环线程退出，之后才没有线程会再访问 clients_，再
    // 向线程池 submit 任务，避免与 clients_.clear() 产生数据竞争
    if (event_thread_.joinable()) {
        event_thread_.join();
    }
    
    // 事件循环退出，销毁线程池
    thread_pool_.reset();
    
    // 事件循环已退出，关闭各 fd
    if (listenfd_ >= 0) {
        ::close(listenfd_);
        listenfd_ = -1;
    }
    if (wakeupfd_ >= 0) {
        ::close(wakeupfd_);
        wakeupfd_ = -1;
    }

    // 关闭 epoll 实例
    if (epollfd_ >= 0) {
        ::close(epollfd_);
        epollfd_ = -1;
    }
    
    // 清空客户端连接，ClientConnection 析构时会自动关闭 socket
    clients_.clear();

    LOG_INFO("Network server stopped");
}

void NetworkServer::eventLoop() {
    // 每次最多处理 1024 个事件
    struct epoll_event events[1024];

    while (running_) {
        int nfds = epoll_wait(epollfd_, events, 1024, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;

            LOG_ERROR("epoll_wait() faild");
            break;
        }

        int fd = -1;
        for (int i = 0; i < nfds; ++i) {
            fd = events[i].data.fd;
            if (fd == wakeupfd_) {
                // shutdown() 已调用，直接退出
                return;
            }
            else if (fd == listenfd_) {
                // 监听 socket 可读，表示有新连接到来
                handleAccept();
            }
            else if (events[i].events & EPOLLIN) {
                // 客户端 socket 可读，表示有数据到来
                handleRead(fd);
            }
            else if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                // 客户端异常或断开连接
                handleDisconnect(fd);
            }
        }
    }
}

void NetworkServer::handleAccept() {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    while (true) {
        int sock = accept(listenfd_, (struct sockaddr*)&client_addr, &client_len);
        if (sock < 0) {
            // 没有新连接，退出
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            LOG_WARN("Failed to accept client connection");
            break;
        }
        // 检查连接数是否超过上限
        if (clients_.size() >= max_connections_) {
            ::close(sock);
            LOG_WARN("Max connections reached, rejected client");
            continue;
        }

        // 将客户端 socket 设置为非阻塞模式
        if (!setNonBlocking(sock)) {
            ::close(sock);
            continue;
        }
        // 添加到 epoll，并设置边缘触发模式
        if (!addEpollEvent(sock, EPOLLIN | EPOLLET)) {
            ::close(sock);
            LOG_WARN("Failed to add client socket to epoll");
            continue;
        }

        // 获取客户端的 IP 地址
        string client_ip = inet_ntoa(client_addr.sin_addr);
        // 创建连接上下文对象，存储在 clients_ 映射中
        clients_[sock] = std::make_unique<ClientConnection>(sock, client_ip);

        LOG_INFO("New client connected: " + client_ip + " (fd:" + std::to_string(sock) + ")");
    }
}

void NetworkServer::handleRead(int client_fd) {
    // 查找客户端连接上下文
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) return;
    ClientConnection* conn = it->second.get();

    while (true) {
        // 读取数据到缓冲区剩余空间
        ssize_t read_bytes = read(client_fd, conn->buffer + conn->buffer_pos, 
            4096 - conn->buffer_pos);
        // 处理无数据读
        if (read_bytes < 0) {
            // EAGAIN/EWOULDBLOCK 表示当前没有数据可读，正常退出
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            LOG_WARN("Read failed for client " + conn->addr);
            // 客户端主动断开连接
            handleDisconnect(client_fd);
            return;
        }
        else if (read_bytes == 0) {
            handleDisconnect(client_fd);
            return;
        }

        // 更新缓冲区位置
        conn->buffer_pos += read_bytes;
        // 以 \n 为分隔符解析数据
        while (true) {
            // 查找换行符
            char* line = reinterpret_cast<char*>(memchr(conn->buffer, '\n', conn->buffer_pos));

            // 没有找到换行符
            if (line == nullptr) {
                // 缓冲区可能满了，检查
                if (conn->buffer_pos >= sizeof(conn->buffer) - 1) {
                    // 缓冲区已满仍无换行：把当前整段当作一行强制处理，防止溢出。
                    // 这类"超长行"无法解析出 seq，按老格式（seq=0，不回 ACK）兜底。
                    handleLine(conn, conn->buffer, conn->buffer_pos);
                    conn->buffer_pos = 0;
                }
                // 数据不完整，自己进行下一次读取
                break;
            }

            // 找到了换行符，提取一行数据（不含换行符）
            size_t line_len = static_cast<size_t>(line - conn->buffer);
            handleLine(conn, conn->buffer, line_len);

            // 移动剩余数据到缓冲区开头
            size_t remaining = conn->buffer_pos - line_len - 1;
            memmove(conn->buffer, line + 1, remaining);
            conn->buffer_pos = remaining;
        }
    }
}

void NetworkServer::handleDisconnect(int fd) {
    auto it = clients_.find(fd);
    if (it != clients_.end()) {
        LOG_INFO("Client disconnected: " + it->second->addr + " (fd:" + std::to_string(fd) + ")");

        // 从 epoll 中移除该文件描述符
        epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, nullptr);
        // 从映射中删除，触发 ClientConnection 析构，自动关闭 socket
        clients_.erase(it);
    }
}

bool NetworkServer::setNonBlocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags < 0) {
        LOG_ERROR("Failed to get socket flags");
        return false;
    }
    
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        LOG_ERROR("Failed to set non-blocking");
        return false;
    }

    return true;
}

bool NetworkServer::addEpollEvent(int sockfd, uint32_t events) {
    struct epoll_event ev;
    ev.data.fd = sockfd;
    ev.events = events;
    
    if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, sockfd, &ev) < 0) {
        LOG_ERROR("Failed to add fd to epoll");
        return false;
    }
    return true;
}

void NetworkServer::handleLine(ClientConnection* conn, char* line_begin, size_t line_len) {
    // 解析一行完整日志。协议帧：
    //   "LOG <seq> <payload>"   —— 可靠模式：处理成功后需回 "ACK <seq>\n"
    //   其它任意文本            —— 兼容老格式：当作 seq=0 的普通日志，不回 ACK
    string device_id = conn->addr;
    int client_fd = conn->sockfd;

    uint64_t seq = 0;
    bool reliable = false;               // 是否需要回 ACK
    string payload(line_begin, line_len);

    // 识别 "LOG " 前缀
    if (line_len > 4 && std::memcmp(line_begin, "LOG ", 4) == 0) {
        const char* p = line_begin + 4;
        const char* end = line_begin + line_len;
        // 解析 seq（十进制无符号整数）
        uint64_t v = 0;
        const char* q = p;
        while (q < end && *q >= '0' && *q <= '9') {
            v = v * 10 + static_cast<uint64_t>(*q - '0');
            ++q;
        }
        // 合法帧要求：seq 后紧跟一个空格，空格后是 payload（payload 可以为空）
        if (q > p && q < end && *q == ' ') {
            seq = v;
            reliable = true;
            payload.assign(q + 1, static_cast<size_t>(end - (q + 1)));
        }
        // 若格式不符（如 "LOG abc"），reliable 保持 false，整行按老格式落库
    }

    if (!cb_ || !thread_pool_) {
        // 没有接线业务回调时，退化为直接打印（保留原有调试行为）
        LOG_INFO("[Device:" + device_id + "] " + payload);
        return;
    }

    // 把落库投递到线程池异步执行；事件循环只负责 I/O。
    // 落库成功且是可靠帧时，直接在该工作线程里回写 ACK。
    thread_pool_->submit([this, device_id, seq, payload, reliable, client_fd]() {
        bool ok = cb_(device_id, seq, payload);
        if (ok && reliable) {
            string ack = "ACK " + std::to_string(seq) + "\n";
            // MSG_NOSIGNAL：对端已关闭时返回 EPIPE 而非触发 SIGPIPE 杀进程
            ::send(client_fd, ack.data(), ack.size(), MSG_NOSIGNAL);
        }
    });
}