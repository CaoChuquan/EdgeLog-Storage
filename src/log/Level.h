#pragma once

#include <string>
#include <cctype>

// 日志级别
enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL    
};

// 把日志级别转为字符串
inline const char* leveltoString(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO";
        case LogLevel::WARN:  return "WARN";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
        default: return "UNKNOW";
    }
}

// 将字符串转换为日志级别枚举
inline LogLevel stringToLevel(const std::string& str) {
    std::string upper_str;
    upper_str.reserve(str.size());
    for (char c : str) {
        upper_str += std::toupper(static_cast<unsigned char>(c));
    }
    if (upper_str == "DEBUG") return LogLevel::DEBUG;
    if (upper_str == "INFO")  return LogLevel::INFO;
    if (upper_str == "WARN")  return LogLevel::WARN;
    if (upper_str == "ERROR") return LogLevel::ERROR;
    if (upper_str == "FATAL") return LogLevel::FATAL;
    return LogLevel::INFO;
}