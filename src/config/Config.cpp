#include "Config.h"
#include <fstream>
#include <iostream>

using std::string;
using nlohmann::json;

void Config::load(const string& config_path) {
    if (loaded_) {
        return;
    }

    std::ifstream file(config_path);
    if (!file.is_open()) return;

    // 把配置数据加载到 json_ 
    try {
        file >> json_;
        loaded_ = true;
    }
    catch (const json::parse_error& e) {
        std::cerr << "Config parse error: " 
            << e.what() << std::endl;
    }

    file.close();
}

string Config::getString(const string& key, const string& value) {
    
}

void Config::parseJson(const string& js) {
    size_t pos = 0;
    string token;

    while (pos < js.size()) {
        
    }
}