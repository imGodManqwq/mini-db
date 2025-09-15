#pragma once
#include "../storage/Row.h"
#include "../storage/Table.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

// 前向声明
class StorageEngine;

// 表的元数据信息
struct TableMetadata
{
    std::string tableName;
    std::vector<ColumnInfo> columns;

    TableMetadata() = default;
    TableMetadata(const std::string &name, const std::vector<ColumnInfo> &cols)
        : tableName(name), columns(cols) {}
};

// 数据库模式目录
class Catalog
{
public:
    Catalog();
    explicit Catalog(StorageEngine *storageEngine);
    ~Catalog() = default;

    // StorageEngine集成
    void setStorageEngine(StorageEngine *storageEngine);
    void syncFromStorage(); // 从StorageEngine同步表信息

    // 表管理
    bool createTable(const std::string &tableName, const std::vector<ColumnInfo> &columns);
    bool dropTable(const std::string &tableName);
    bool tableExists(const std::string &tableName) const;
    TableMetadata *getTableMetadata(const std::string &tableName);
    const TableMetadata *getTableMetadata(const std::string &tableName) const;

    // 列管理
    bool columnExists(const std::string &tableName, const std::string &columnName) const;
    DataType getColumnType(const std::string &tableName, const std::string &columnName) const;
    int getColumnIndex(const std::string &tableName, const std::string &columnName) const;
    const std::vector<ColumnInfo> &getTableColumns(const std::string &tableName) const;

    // 数据类型检查
    bool isValidType(DataType type) const;
    bool isTypeCompatible(DataType columnType, DataType valueType) const;

    // 值类型转换和验证
    bool validateValue(const Value &value, DataType expectedType) const;
    Value convertValue(const Value &value, DataType targetType) const;

    // 工具函数
    void printCatalog() const;
    std::vector<std::string> getTableNames() const;
    size_t getTableCount() const;

    // 持久化
    bool saveToFile(const std::string &filename) const;
    bool loadFromFile(const std::string &filename);

private:
    std::unordered_map<std::string, TableMetadata> tables_;
    StorageEngine *storageEngine_; // 关联的存储引擎（可为空）

    // 内部辅助函数
    std::string dataTypeToString(DataType type) const;
    DataType stringToDataType(const std::string &typeStr) const;
    void updateFromStorageEngine(); // 内部更新方法
};
