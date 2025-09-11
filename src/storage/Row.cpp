#include "../../include/storage/Row.h"
#include <sstream>
#include <stdexcept>

Row::Row(const std::vector<Value>& values) : values_(values) {}

void Row::addValue(const Value& value) {
    values_.push_back(value);
}

const Value& Row::getValue(size_t index) const {
    if (index >= values_.size()) {
        throw std::out_of_range("Row field index out of range");
    }
    return values_[index];
}

Value& Row::getValue(size_t index) {
    if (index >= values_.size()) {
        throw std::out_of_range("Row field index out of range");
    }
    return values_[index];
}

size_t Row::getFieldCount() const {
    return values_.size();
}

std::string Row::serialize() const {
    std::ostringstream oss;
    oss << values_.size() << "|";
    
    for (const auto& value : values_) {
        std::visit([&oss](const auto& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, int>) {
                oss << "I" << v << "|";
            } else if constexpr (std::is_same_v<T, std::string>) {
                oss << "S" << v.length() << ":" << v << "|";
            } else if constexpr (std::is_same_v<T, double>) {
                oss << "D" << v << "|";
            }
        }, value);
    }
    
    return oss.str();
}

Row Row::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string token;
    
    // 读取字段数量
    std::getline(iss, token, '|');
    size_t fieldCount = std::stoull(token);
    
    Row row;
    for (size_t i = 0; i < fieldCount; ++i) {
        std::getline(iss, token, '|');
        if (token.empty()) break;
        
        char type = token[0];
        std::string valueStr = token.substr(1);
        
        switch (type) {
            case 'I': {
                int intValue = std::stoi(valueStr);
                row.addValue(intValue);
                break;
            }
            case 'S': {
                size_t colonPos = valueStr.find(':');
                if (colonPos != std::string::npos) {
                    std::string strValue = valueStr.substr(colonPos + 1);
                    row.addValue(strValue);
                }
                break;
            }
            case 'D': {
                double doubleValue = std::stod(valueStr);
                row.addValue(doubleValue);
                break;
            }
        }
    }
    
    return row;
}

std::string Row::toString() const {
    std::ostringstream oss;
    oss << "(";
    
    for (size_t i = 0; i < values_.size(); ++i) {
        if (i > 0) oss << ", ";
        
        std::visit([&oss](const auto& v) {
            oss << v;
        }, values_[i]);
    }
    
    oss << ")";
    return oss.str();
}

bool Row::operator==(const Row& other) const {
    return values_ == other.values_;
}
