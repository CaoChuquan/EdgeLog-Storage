#include "ReliableClient.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <cstring>
#include <cerrno>
#include <chrono>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

bool ReliableClient::start(const std::string& wal_path,
                           const std::string& server_host, int server_port,
                           const std::string& device_id) {
    wal_path_  = wal_path;
    host_      = server_host;
    port_      = server_port;
    device_id_ = device_id;

    // 1) 先回放 WAL：把上次运行遗留的、未确认的日志恢复进 unacked_，
    //    并把 next_seq_ 推进到已见过的最大 seq 之后，避免 seq 重复
    if (!replayWal()) {
        std::cerr << "[ReliableClient] replay WAL failed: " << wal_path_ << "\n";
        return false;
    }

    // 2) 启动后台发送线程。恢复出来的未确认日志会在首轮连接建立后立即重传
    running_ = true;
    send_thread_ = std::thread(&ReliableClient::sendLoop, this);
    return true;
}

void ReliableClient::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;   // 已停，幂等
    }
    cv_.notify_all();               // 唤醒可能在等待新日志的发送线程
    if (send_thread_.joinable()) {
        send_thread_.join();
    }
}

// WAL 是按行组织的文本，payload 里若含换行或反斜杠会破坏行结构，做最小转义：
//   '\\'  -> "\\\\"      （反斜杠自身）
//   '\n'  -> "\\n"       （换行编码成两字符 backslash+n）
// 其余字符原样保留（空格无需转义，反解析时按第一个/第二个空格切分字段）
std::string ReliableClient::escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '\\')      out += "\\\\";
        else if (c == '\n') out += "\\n";
        else                out += c;
    }
    return out;
}

std::string ReliableClient::unescape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            char n = s[++i];
            if (n == '\\')      out += '\\';
            else if (n == 'n')  out += '\n';
            else                out += n;   // 未知转义，容错保留
        } else {
            out += s[i];
        }
    }
    return out;
}

// WAL 读写 
bool ReliableClient::appendWal(const std::string& line) {
    // 以追加模式打开，写完 flush 并 fsync，确保记录真正落到磁盘介质，
    // 这样"先写 WAL 再返回"才真正抗掉电。频繁 fsync 有开销，但这是可靠性的代价；
    // 后续可做"批量日志一次 fsync"来平衡吞吐
    std::ofstream ofs(wal_path_, std::ios::app | std::ios::binary);
    if (!ofs.is_open()) {
        return false;
    }
    ofs << line << '\n';
    ofs.flush();
    if (!ofs.good()) {
        return false;
    }
    // std::ofstream 没有直接的 fsync；用底层文件描述符刷盘
    // 简单起见这里重新以只读方式拿 fd 做 fsync 的成本较高，改为在关闭前依赖 flush
    // 为确保介质级持久化，单独打开并 fsync：
    ofs.close();
    int fd = ::open(wal_path_.c_str(), O_WRONLY | O_APPEND);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
    return true;
}

bool ReliableClient::replayWal() {
    std::ifstream ifs(wal_path_, std::ios::binary);
    if (!ifs.is_open()) {
        // WAL 不存在属正常情况（首次启动）：无历史可恢复，直接返回成功
        return true;
    }

    uint64_t max_seq = 0;
    std::string line;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;

        // 记录类型由行首字符决定：'P'=PUT，'A'=ACK
        char type = line[0];
        if (type == 'P') {
            // 格式：P <seq> <escaped_payload>
            // 用前两个空格切出 seq，其余全部是 payload（payload 本身可能含空格）
            size_t sp1 = line.find(' ', 0);              // 'P' 后的空格
            size_t sp2 = (sp1 == std::string::npos) ? std::string::npos
                                                     : line.find(' ', sp1 + 1);
            if (sp1 == std::string::npos || sp2 == std::string::npos) continue;
            uint64_t seq = std::strtoull(line.c_str() + sp1 + 1, nullptr, 10);
            std::string payload = unescape(line.substr(sp2 + 1));
            unacked_[seq] = payload;                     // 先视为未确认
            if (seq > max_seq) max_seq = seq;
        } else if (type == 'A') {
            // 格式：A <seq> —— 该 seq 已确认，从待发集合逻辑删除
            size_t sp1 = line.find(' ', 0);
            if (sp1 == std::string::npos) continue;
            uint64_t seq = std::strtoull(line.c_str() + sp1 + 1, nullptr, 10);
            unacked_.erase(seq);
            if (seq > max_seq) max_seq = seq;
        }
    }

    // 新分配的 seq 必须大于历史上出现过的任何 seq，避免与旧记录冲突
    next_seq_ = max_seq + 1;
    return true;
}

// 上报日志
bool ReliableClient::log(const std::string& payload) {
    uint64_t seq;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        seq = next_seq_++;

        // 关键顺序：先把 PUT 记录写进 WAL（落盘）再纳入 unacked_
        // 这样即便写完 WAL 后立刻崩溃，重启回放也能恢复这条日志；
        // 若 WAL 写失败则不推进状态，向调用方报错
        std::string rec = "P " + std::to_string(seq) + " " + escape(payload);
        if (!appendWal(rec)) {
            --next_seq_;   // 回退分配的 seq
            return false;
        }
        unacked_[seq] = payload;
    }
    cv_.notify_one();   // 唤醒发送线程尽快发出
    return true;
}

size_t ReliableClient::pendingCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return unacked_.size();
}

// 后台发送线程 
int ReliableClient::connectServer() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    // 关闭 Nagle：日志多为小包，低延迟比合并更重要，也让 ACK 往返更快
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool ReliableClient::sendAllUnacked(int fd) {
    // 快照一份待发列表（拷贝出临界区，避免持锁做网络 I/O）
    // map 有序遍历 => 按 seq 升序发送，重传也保持产生顺序
    std::vector<std::pair<uint64_t, std::string>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot.assign(unacked_.begin(), unacked_.end());
    }

    for (auto& kv : snapshot) {
        // 帧格式与服务端协议一致：LOG <seq> <payload>\n
        std::string frame = "LOG " + std::to_string(kv.first) + " " + kv.second + "\n";
        size_t sent = 0;
        while (sent < frame.size()) {
            ssize_t n = ::send(fd, frame.data() + sent, frame.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                return false;   // 连接异常，交由上层重连
            }
            sent += static_cast<size_t>(n);
        }
    }
    return true;
}

bool ReliableClient::drainAcks(int fd, int timeout_ms) {
    // 用 poll 等待 ACK 数据，超时后返回（让上层有机会发新数据/检查停机）
    // 收到 "ACK <seq>\n" 就确认该 seq：从 unacked_ 删除，并向 WAL 追加 A 记录
    struct pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN;

    static thread_local std::string rbuf;   // 跨调用保留半行，处理 TCP 粘包/拆包

    int pr = ::poll(&pfd, 1, timeout_ms);
    if (pr < 0) {
        if (errno == EINTR) return true;
        return false;
    }
    if (pr == 0) {
        return true;   // 超时无数据，连接仍视为正常
    }
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return false;
    }

    char buf[4096];
    ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
    if (n == 0) return false;          // 对端关闭
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
        return false;
    }
    rbuf.append(buf, static_cast<size_t>(n));

    // 按行解析累积缓冲
    size_t pos;
    while ((pos = rbuf.find('\n')) != std::string::npos) {
        std::string line = rbuf.substr(0, pos);
        rbuf.erase(0, pos + 1);
        if (line.size() > 4 && std::memcmp(line.data(), "ACK ", 4) == 0) {
            uint64_t seq = std::strtoull(line.c_str() + 4, nullptr, 10);
            bool erased = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                erased = (unacked_.erase(seq) > 0);
            }
            if (erased) {
                // 追加 ACK 记录到 WAL：标记该 seq 已确认，重启回放时会据此不再重发
                appendWal("A " + std::to_string(seq));
                acked_count_.fetch_add(1);
            }
        }
    }
    return true;
}

void ReliableClient::sendLoop() {
    int fd = -1;

    while (running_) {
        // 1) 确保连接可用：断开则重连（弱网下反复重连是常态）
        if (fd < 0) {
            fd = connectServer();
            if (fd < 0) {
                // 连不上：退避后重试。期间若被 stop() 唤醒则尽快退出
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(lock, std::chrono::milliseconds(500),
                             [this] { return !running_.load(); });
                continue;
            }
            // 新连接建立：立刻把所有未确认日志重传一遍（含崩溃恢复出来的历史日志）
            if (!sendAllUnacked(fd)) {
                ::close(fd);
                fd = -1;
                continue;
            }
        }

        // 2) 若有待确认日志，读 ACK 推进确认；否则等待新日志或停机
        bool has_pending;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            has_pending = !unacked_.empty();
        }

        if (has_pending) {
            // 有在途日志：读一会儿 ACK。读到连接异常则重连并重发
            if (!drainAcks(fd, 200)) {
                ::close(fd);
                fd = -1;
                continue;
            }
            // 期间可能又产生了新日志（log() 追加到了 unacked_），发出去
            if (!sendAllUnacked(fd)) {
                ::close(fd);
                fd = -1;
                continue;
            }
        } else {
            // 无待确认：睡到有新日志被 log() 唤醒，或停机
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !unacked_.empty() || !running_.load(); });
            lock.unlock();
            // 醒来后若有新日志，下一轮循环顶部会走 sendAllUnacked
            if (running_ && fd >= 0) {
                if (!sendAllUnacked(fd)) {
                    ::close(fd);
                    fd = -1;
                }
            }
        }
    }

    if (fd >= 0) {
        ::close(fd);
    }
}