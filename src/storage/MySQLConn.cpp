#include "MySQLConn.h"

using std::string;
using std::vector;
using std::move;

MySQLConn::MySQLConn(MySQLConn&& other) noexcept
    : conn_(other.conn_)
    , last_error_(move(other.last_error_))
{
    other.conn_ = conn_;
}

MySQLConn& MySQLConn::operator=(MySQLConn&& other) noexcept {
    if (this != &other) {
        close();
        conn_ = other.conn_;
        last_error_ = move(other.last_error_);
        other.conn_ = nullptr;
    }
    return *this;
}

void MySQLConn::close() {
    if (conn_) {
        mysql_close(conn_);
        conn_ = nullptr;
    }
}

bool MySQLConn::connect(const char* host, const char* user, const char* password,
                        const char* database, unsigned int port)
{
    close();

    // 初始化 MySQL 对象
    conn_ = mysql_init(nullptr);
    if (!conn_) {
        last_error_ = "mysql_init() failed";
        return false;
    }
    // 设置连接字符集为 utf8mb4
    mysql_options(conn_, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    // 连接数据库
    if (!mysql_real_connect(conn_, host, user, password, database,
                            port, nullptr, 0)) 
    {
        last_error_ = mysql_error(conn_);
        close();
        return false;
    }

    return true;
}

bool MySQLConn::execute(const char* sql) {
    if (conn_) {
        last_error_ = "not connect";
        return false;
    }

    // 执行 sql 语句
    if (mysql_real_query(conn_, sql, sizeof(sql)) != 0) {
        last_error_ = mysql_error(conn_);
        last_errno_ = mysql_errno(conn_);
        return false;
    }
    last_errno_ = 0;
    return true;
}

bool MySQLConn::query(const char* sql, vector<vector<string>>& out) {
    out.clear();
    if (!conn_) {
        last_error_ = "not connected";
        return false;
    }

    // 执行 sql 语句
    if (mysql_real_query(conn_, sql, sizeof(sql)) != 0) {
        last_error_ = mysql_error(conn_);
        return false;
    }

    MYSQL_RES* res = mysql_store_result(conn_);
    if (!res) {
        if (mysql_field_count(conn_) == 0) {
            return true;   // 语句本身不返回结果集
        }

        last_error_ = mysql_error(conn_);
        return false;
    }

    unsigned int cols = mysql_num_fields(res);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        unsigned long* lengths = mysql_fetch_lengths(res);
        vector<string> record;
        record.reserve(cols);
        for (unsigned int i = 0; i < cols; ++i) {
            if (row[i]) {
                record.emplace_back(row[i], lengths[i]);
            }
            else {
                record.emplace_back();   // NULL 以空串表示
            }
        }
        out.push_back(std::move(record));
    }

    mysql_free_result(res);
    return true;
}

unsigned long long MySQLConn::lastInsertId() const {
    if (!conn_) return 0;
    return mysql_insert_id(conn_);
}

string MySQLConn::escape(const string& in) const {
    if (!conn_) return in;
    // 最坏情况下每个字符被转义成两个，末尾留 '\0'
    string out;
    out.resize(in.size() * 2 + 1);
    unsigned long len = mysql_real_escape_string(
        conn_, &out[0], in.c_str(), in.size());
    out.resize(len);
    return out;
}