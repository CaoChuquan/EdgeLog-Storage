// 网络层端到端回归测试
//   在同一进程内：启动 NetworkServer -> 客户端连接 -> 发送多行日志 ->
//   验证回调收到全部行并写入 AsyncLogger -> 有序 shutdown。
// 用于回归验证设备 -> 网络 -> 线程池 -> 日志整条链路与停机时序的正确性。
#include "AsyncLogger.h"
#include "Server.h"
#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

int main() {
    AsyncLogger::instance().init("./logs_test", LogLevel::DEBUG);

    std::atomic<int> received{ 0 };
    NetworkServer::instance().setLogDataCallback(
        [&received](std::string device_id, uint64_t seq, std::string log_data) -> bool {
            LOG_INFO("[Device:" + device_id + " seq:" + std::to_string(seq) + "] " + log_data);
            received.fetch_add(1);
            return true;
        });

    const int port = 9099;
    if (!NetworkServer::instance().init(port)) {
        std::cerr << "server init failed\n";
        return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 客户端：连接并发送 3 行以 \n 分隔的日志
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) != 0) {
        std::cerr << "client connect failed\n";
        return 2;
    }
    const char* payload = "temperature=25.3\nhumidity=60\ncpu_load=0.75\n";
    ::send(fd, payload, std::strlen(payload), 0);

    // 等待服务器 + 线程池处理完全部 3 行（最多 2.5s）
    for (int i = 0; i < 50 && received.load() < 3; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ::close(fd);

    NetworkServer::instance().shutdown();
    AsyncLogger::instance().shutdown();

    int got = received.load();
    std::cout << "callback received = " << got << " lines\n";
    if (got != 3) {
        std::cerr << "FAILED: expected 3 lines, got " << got << "\n";
        return 3;
    }
    std::cout << "PASSED\n";
    return 0;
}