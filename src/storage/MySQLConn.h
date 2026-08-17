#pragma once

#include <mysql/mysql.h>
#include <string>
#include <vector>

// 对 MySQL C API 的封装
class MySQLConn {
public:
    MySQLConn() {}
    ~MySQLConn() { close(); }

    // 不可复制，可移动
    MySQLConn(const MySQLConn&) = delete;
    MySQLConn& operator=(const MySQLConn&) = delete;
    MySQLConn(MySQLConn&& other) noexcept;
    MySQLConn& operator=(MySQLConn&& other) noexcept;

    // 连接数据库，连接字符集固定为 utf8mb4
    bool connect(const char* host, const char* user, const char* password,
                 const char* database, unsigned int port);
    // 执行不返回结果的语句（INSERT/UPDATE/DELETE 等）
    bool execute(const char* sql);
    // 执行 select，返回二维字符串表（每行一个 vector<string>）
    // 查询失败返回 false，成功时结果写入 out
    bool query(const char* sql, std::vector<std::vector<std::string>>& out);
    // 返回最近一次 insert 产生的自增 id
    unsigned long long lastInsertId() const;
    // 对字符串做 SQL 转义，返回可安全嵌入单引号中的内容
    std::string escape(const std::string& in) const;

    bool isConnected() const { return conn_ != nullptr; }
    const std::string& lastError() const { return last_error_; }
    unsigned int lastErrno() const { return last_errno_; }
private:
    void close();

    MYSQL* conn_{ nullptr };
    std::string last_error_;
    unsigned int last_errno_{ 0 }; // 最近一次操作的 MySQL 错误码
};