#pragma once
#include <vector>
#include <string>
#include <variant>

// 支持的数据类型
enum class DataType {
    INT,
    STRING,
    DOUBLE
};

// 数据值的变体类型
using Value = std::variant<int, std::string, double>;

class Row {
public:
    Row() = default;
    explicit Row(const std::vector<Value>& values);
    
    // 添加字段值
    void addValue(const Value& value);
    
    // 获取字段值
    const Value& getValue(size_t index) const;
    Value& getValue(size_t index);
    
    // 获取字段数量
    size_t getFieldCount() const;
    
    // 序列化和反序列化（用于存储）
    std::string serialize() const;
    static Row deserialize(const std::string& data);
    
    // 打印行数据
    std::string toString() const;
    
    // 比较操作
    bool operator==(const Row& other) const;
    
private:
    std::vector<Value> values_;
};
