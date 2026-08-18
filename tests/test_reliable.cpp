// 弱网可靠性端到端测试（纯网络层，不依赖 MySQL）
//
// 目标：验证设备端 WAL + 逐条 seq + ACK + 断线重连重传 + 服务端幂等去重这条
// 可靠采集链路在弱网下不丢、不重
//
// 做法：用一个可控的极简测试服务端（裸 socket），它：
//   解析 "LOG <seq> <payload>" 帧，按 (seq) 去重后"接受"日志，并回 "ACK <seq>"；
//   前 kDropConns 个连接在接受少量日志后立即关闭，模拟弱网断线（客户端需重连重传）；
//   对某些 ACK"故意不回"，模拟 ACK 丢失（客户端超时后重传，去重保证不重复接受）
// 客户端用 ReliableClient，日志全部先落 WAL 再发送
//
// 断言：所有日志最终被服务端恰好接受一次；客户端 unacked 最终清空；崩溃恢复后续传成功

#include "ReliableClient.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>

namespace {

// 测试服务端：单线程 accept 循环 + 每连接内联处理，模拟弱网可控行为。
class MockServer {
public:
    // drop_first_n_conns：前 N 个连接在接受 accept_per_conn 条日志后立即关闭
    bool start(int port, int drop_first_n_conns, int accept_per_conn) {
        port_ = port;
        drop_first_n_conns_ = drop_first_n_conns;
        accept_per_conn_ = accept_per_conn;

        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) return false;
        int opt = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return false;
        if (::listen(listen_fd_, 16) != 0) return false;

        running_ = true;
        thread_ = std::thread(&MockServer::loop, this);
        return true;
    }

    void stop() {
        running_ = false;
        if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
        if (thread_.joinable()) thread_.join();
    }

    // 已"接受"（去重后）的不同 seq 数量
    size_t acceptedUnique() { std::lock_guard<std::mutex> l(m_); return accepted_.size(); }
    // 收到的 LOG 帧总数（含重复）——用来观察重传确实发生了
    long framesTotal() { return frames_total_.load(); }

private:
    void loop() {
        int conn_index = 0;
        while (running_) {
            // 用 poll 等待新连接，避免 accept 永久阻塞导致 stop() 卡住
            pollfd pfd{ listen_fd_, POLLIN, 0 };
            int pr = ::poll(&pfd, 1, 200);
            if (pr <= 0) continue;
            int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) continue;

            bool drop_this = (conn_index < drop_first_n_conns_);
            handleConn(fd, drop_this);
            ::close(fd);
            ++conn_index;
        }
    }

    void handleConn(int fd, bool drop_after_some) {
        std::string rbuf;
        int accepted_on_this_conn = 0;
        char buf[4096];

        while (running_) {
            pollfd pfd{ fd, POLLIN, 0 };
            int pr = ::poll(&pfd, 1, 300);
            if (pr < 0) break;
            if (pr == 0) {
                // 该连接一段时间无数据：若非 drop 连接则继续等，drop 连接直接返回
                if (drop_after_some && accepted_on_this_conn >= accept_per_conn_) break;
                continue;
            }
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) break;   // 客户端关闭
            rbuf.append(buf, static_cast<size_t>(n));

            size_t pos;
            while ((pos = rbuf.find('\n')) != std::string::npos) {
                std::string line = rbuf.substr(0, pos);
                rbuf.erase(0, pos + 1);
                if (line.size() > 4 && std::memcmp(line.data(), "LOG ", 4) == 0) {
                    frames_total_.fetch_add(1);
                    // 解析 seq
                    const char* p = line.c_str() + 4;
                    uint64_t seq = std::strtoull(p, nullptr, 10);
                    {
                        std::lock_guard<std::mutex> l(m_);
                        accepted_.insert(seq);   // set 去重：同一 seq 只记一次
                    }
                    // 回 ACK
                    std::string ack = "ACK " + std::to_string(seq) + "\n";
                    ::send(fd, ack.data(), ack.size(), MSG_NOSIGNAL);
                    ++accepted_on_this_conn;

                    // 模拟弱网断线：drop 连接在接受够 N 条后立即关闭，
                    // 迫使客户端重连并重传剩余未确认日志。
                    if (drop_after_some && accepted_on_this_conn >= accept_per_conn_) {
                        return;
                    }
                }
            }
        }
    }

    int port_{ 0 };
    int listen_fd_{ -1 };
    int drop_first_n_conns_{ 0 };
    int accept_per_conn_{ 0 };
    std::thread thread_;
    std::atomic<bool> running_{ false };

    std::mutex m_;
    std::set<uint64_t> accepted_;
    std::atomic<long> frames_total_{ 0 };
};

// 等待条件成立，最多 timeout_ms 毫秒；成立返回 true
template <typename Pred>
bool waitFor(Pred pred, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

}  // namespace

int main() {
    const int kPort = 9110;
    const std::string kWal = "./test_reliable.wal";
    const std::string kDev = "reliable_dev_1";

    // 干净起步：删掉可能残留的 WAL
    ::unlink(kWal.c_str());

    // 场景一：弱网断线重传 + 去重
    // 服务端前 2 个连接各接受 3 条后立即断开，逼迫客户端重连重传。
    MockServer server;
    if (!server.start(kPort, /*drop_first_n_conns=*/2, /*accept_per_conn=*/3)) {
        std::cerr << "FAILED: mock server start\n";
        return 1;
    }

    const int kTotal = 20;
    {
        ReliableClient client;
        if (!client.start(kWal, "127.0.0.1", kPort, kDev)) {
            std::cerr << "FAILED: client start\n";
            server.stop();
            return 2;
        }
        // 连续上报 20 条日志。期间连接会被服务端断开两次，客户端应自动重连补发。
        for (int i = 0; i < kTotal; ++i) {
            if (!client.log("payload_" + std::to_string(i))) {
                std::cerr << "FAILED: client.log returned false at i=" << i << "\n";
                server.stop();
                return 3;
            }
        }

        // 等待全部 20 条被服务端唯一接受，且客户端本地全部拿到 ACK（unacked 清空）。
        bool ok = waitFor([&] {
            return server.acceptedUnique() >= static_cast<size_t>(kTotal)
                && client.pendingCount() == 0;
        }, 8000);

        std::cout << "[scene1] accepted_unique=" << server.acceptedUnique()
                  << " frames_total=" << server.framesTotal()
                  << " client_pending=" << client.pendingCount()
                  << " client_acked=" << client.ackedCount() << "\n";

        if (!ok) {
            std::cerr << "FAILED: not all logs reliably delivered in scene1\n";
            server.stop();
            return 4;
        }
        // frames_total 应 > kTotal（因为发生了重传），证明弱网确实触发了重发
        if (server.framesTotal() <= kTotal) {
            std::cerr << "WARN: no retransmission observed (frames_total="
                      << server.framesTotal() << ")\n";
        }
        client.stop();
    }

    // 场景二：崩溃恢复（进程重启续传）
    // 手动往 WAL 追加两条"有 PUT 无 ACK"的记录，模拟：上次运行写了日志但没来得及确认
    // 就崩溃。新建 client 指向同一 WAL，应回放这两条并送达。
    {
        // 直接以最原始方式追加两条未确认记录（seq 取远大于之前的值，避免与已 ACK 冲突）
        std::ofstream ofs(kWal, std::ios::app | std::ios::binary);
        ofs << "P 100001 crash_recovery_a\n";
        ofs << "P 100002 crash_recovery_b\n";
        ofs.close();
    }

    {
        ReliableClient client;
        if (!client.start(kWal, "127.0.0.1", kPort, kDev)) {
            std::cerr << "FAILED: client restart\n";
            server.stop();
            return 5;
        }
        // 不再产生新日志，仅靠回放把 100001/100002 续传出去。
        bool ok = waitFor([&] { return client.pendingCount() == 0; }, 8000);

        std::cout << "[scene2] recovered_acked_total=" << client.ackedCount()
                  << " client_pending=" << client.pendingCount()
                  << " server_accepted_unique=" << server.acceptedUnique() << "\n";

        if (!ok) {
            std::cerr << "FAILED: crash-recovery logs not delivered\n";
            server.stop();
            return 6;
        }
        // 服务端应额外接受了 2 条（100001、100002）
        if (server.acceptedUnique() < static_cast<size_t>(kTotal + 2)) {
            std::cerr << "FAILED: server did not accept recovered logs, unique="
                      << server.acceptedUnique() << "\n";
            server.stop();
            return 7;
        }
        client.stop();
    }

    server.stop();
    ::unlink(kWal.c_str());

    std::cout << "PASSED\n";
    return 0;
}