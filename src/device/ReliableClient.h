#pragma once

#include <string>
#include <cstdint>
#include <map>
#include <mutex>
#include <thread>
#include <atomic>
#include <condition_variable>

/*
设备端可靠上报 SDK（弱网可靠采集的"设备侧"）

要解决的问题：边缘设备处在弱网中，TCP 连接随时可能断，直接 send() 出去的日志
无从得知是否真的被服务端持久化。断线、甚至设备进程崩溃/掉电，都不能丢日志

核心机制（三件事合起来构成可靠闭环）：
  1. 先写 WAL 再发送：
     log() 产生一条日志时，先把它以 append-only 方式落到本地磁盘 WAL 文件，
     落盘成功才返回。即使立刻断网/崩溃，这条日志也已在磁盘上，不会丢
  2. 发送 + 等 ACK：
     后台发送线程把 WAL 中"未确认"的日志逐条发给服务端（帧："LOG <seq> <payload>"），
     收到服务端回的 "ACK <seq>" 才把该 seq 标记为已确认，并在 WAL 里追加一条 ACK 记录
  3. 断线重连 + 崩溃恢复：
     连接断开或读 ACK 超时就自动重连，重连后把所有"未确认"日志重新发一遍（重传）
     进程重启时先回放 WAL：把"有 PUT 但无对应 ACK"的日志重新纳入待发集合，从磁盘续传

服务端侧用 (device_id, seq) 幂等去重，所以重传导致的重复帧不会造成重复落库

WAL 文件格式（每行一条记录，append-only）：
    P <seq> <escaped_payload>\n     -- PUT：产生一条日志
    A <seq>\n                        -- ACK：seq 已确认，可从待发集合逻辑删除
payload 做极简转义（'\\'->"\\\\"，换行->"\\n"）以保证单行完整、便于回放解析
 */
class ReliableClient {
public:
    ReliableClient() { }
    ~ReliableClient() { stop(); }

    ReliableClient(const ReliableClient&) = delete;
    ReliableClient& operator=(const ReliableClient&) = delete;

    // 启动客户端：
    //   wal_path        本地 WAL 文件路径（设备掉电/重启后从这里恢复）
    //   server_host/port 服务端地址
    //   device_id        设备标识（写入日志帧，供服务端归档/去重）
    // 会先回放 WAL 恢复未确认日志，再启动后台发送线程。成功返回 true
    bool start(const std::string& wal_path,
               const std::string& server_host, int server_port,
               const std::string& device_id);

    // 停止客户端：置停机标志、唤醒并 join 发送线程、关闭连接。可安全重复调用
    void stop();

    // 上报一条日志：先写 WAL（同步落盘），再唤醒发送线程去发
    // 返回 false 仅表示"写本地 WAL 失败"（磁盘问题）；网络是否可达不影响返回值——
    // 只要进了 WAL 就保证最终会被可靠送达（弱网下的关键语义）
    bool log(const std::string& payload);

    // 已成功收到 ACK 的日志条数（测试/监控用）
    uint64_t ackedCount() const { return acked_count_.load(); }
    // 当前仍未确认（待发/在途）的日志条数
    size_t pendingCount();

private:
    // WAL 记录的转义/反转义：把 payload 里的 '\\' 与换行编码成单行安全形式
    static std::string escape(const std::string& s);
    static std::string unescape(const std::string& s);

    // 回放 WAL 文件，重建 unacked_ 与 next_seq_（崩溃/重启恢复）
    bool replayWal();
    // 向 WAL 追加一行并 flush + fsync，保证落盘（写失败返回 false）
    bool appendWal(const std::string& line);

    // 后台发送线程主体：维护连接、发送未确认日志、读取并处理 ACK、断线重连
    void sendLoop();
    // 建立到服务端的 TCP 连接（阻塞 connect），成功返回 fd，失败返回 -1
    int connectServer();
    // 把 unacked_ 中所有未确认日志按 seq 升序发送一遍（重传/首发共用）
    bool sendAllUnacked(int fd);
    // 从 socket 读取并解析 "ACK <seq>" 行，逐个确认对应 seq。返回是否连接仍正常。
    bool drainAcks(int fd, int timeout_ms);

    std::string wal_path_;
    std::string host_;
    int port_{ 0 };
    std::string device_id_;

    // 这是发送与重传的唯一依据；收到 ACK 后从这里删除。用 map 保证按 seq 有序遍历，
    // 让重传天然按产生顺序进行。
    std::map<uint64_t, std::string> unacked_;
    std::mutex mutex_;                 // 保护 unacked_ / next_seq_ / WAL 追加
    std::condition_variable cv_;       // 有新日志或停机时唤醒发送线程

    uint64_t next_seq_{ 1 };           // 下一条日志分配的 seq（从 1 开始，0 保留给老格式）

    std::thread send_thread_;
    std::atomic<bool> running_{ false };
    std::atomic<uint64_t> acked_count_{ 0 };
};