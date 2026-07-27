#include "Config.h"
#include <fstream>
#include <iostream>

using std::string;
using nlohmann::json;

void Config::load(const string& config_path) {
    // 检查配置文件是否已被加载
    if (loaded_) {
        return;
    }

    std::ifstream file(config_path);
    if (!file.is_open()) {
        std::cerr << "Configuration file loading error\n";
        return;
    }

    // 把配置数据加载到 json_ 
    try {
        file >> json_;
        loaded_ = true;
    }
    catch (const json::parse_error& e) {
        std::cerr << "Config parse error: " 
            << e.what() << "\n";
    }

    file.close();
}

string Config::getString(const string& key, const string& value) {
    if (json_.contains(key) && json_[key].is_string()) {
        return json_[key].get<string>();
    }
    return value;
}

int Config::getInt(const string& key, int value) {
    if (json_.contains(key) && json_[key].is_number_integer()) {
        return json_[key].get<int>();
    }
    return value;
}

unsigned int Config::getUInt(const string& key, unsigned int value) {
    if (json_.contains(key) && json_[key].is_number_unsigned()) {
        return json_[key].get<unsigned int>();
    }
    return value;
}

bool Config::getBool(const string& key, bool value) {
    if (json_.contains(key) && json_[key].is_boolean()) {
        return json_[key].get<bool>();
    }
    return value;
}