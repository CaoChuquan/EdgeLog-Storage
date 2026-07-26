#pragma once

#include <string>
#include "json.hpp"

/*
使用单例模式实现的配置管理模块，负责从配置文件中加载配置项。
配置文件采用 JSON 格式，支持键值对的读取。
 
设计要点：
    单例模式，全局唯一访问点
    延迟加载，首次访问时读取配置文件
    提供类型安全的配置项获取接口
    支持默认值，配置文件中不存在时返回默认值
 */
class Config {
public:
    static Config& instance() {
        static Config ins;
        return ins;
    }
    // 加载配置文件
    void load(const std::string& config_path = "./config.json");
    // 获取配置项的 string 类型
    std::string getString(const std::string& key, const std::string& value = "");
    // 获取配置项的 int 类型
    int getInt(const std::string& key, int vaule = 0);
    // 获取配置项的 unsigned int 类型
    unsigned int getUInt(const std::string& key, unsigned int value = 0);
    // 获取配置项的 unsigned int 类型
    bool getBool(const std::string& key, bool value = false);
private:
    Config() {}
    void parseJson(const std::string& js);

    nlohmann::json json_;
    bool loaded_{ false };
};