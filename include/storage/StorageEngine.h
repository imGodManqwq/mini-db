#pragma once
#include "Table.h"
#include "PageManager.h"
#include "IndexManager.h"
#include <unordered_map>
#include <memory>
#include <string>

class StorageEngine {
public:
    explicit StorageEngine(const std::string& dbPath);
    ~StorageEngine();
    
    // 表管理
    bool createTable(const std::string& tableName, const std::vector<ColumnInfo>& columns);
    bool dropTable(const std::string& tableName);
    std::shared_ptr<Table> getTable(const std::string& tableName);
    std::shared_ptr<const Table> getTable(const std::string& tableName) const;
    bool tableExists(const std::string& tableName) const;
    std::vector<std::string> getAllTableNames() const;
    
    // 数据操作
    bool insertRow(const std::string& tableName, const Row& row);
    bool insertRow(const std::string& tableName, const std::vector<Value>& values);
    // 批量插入优化
    size_t batchInsertRows(const std::string& tableName, const std::vector<std::vector<Value>>& batchData);
    // 快速批量插入（不更新索引，需要手动重建索引）
    size_t fastBatchInsertRows(const std::string& tableName, const std::vector<std::vector<Value>>& batchData);
    bool deleteRow(const std::string& tableName, const Row& row, uint32_t recordId);
    bool updateRow(const std::string& tableName, const Row& oldRow, const Row& newRow, uint32_t recordId);
    
    // 索引操作
    bool createIndex(const std::string& indexName, const std::string& tableName, 
                    const std::string& columnName, bool isUnique = false);
    bool dropIndex(const std::string& indexName);
    // 重建表的所有索引
    void rebuildTableIndexes(const std::string& tableName);
    // 强制写入所有脏页面
    void flushAllPages();
    // 检查索引是否存在
    bool indexExists(const std::string& indexName) const;
    
    // 查询操作（支持索引）
    std::vector<uint32_t> searchByIndex(const std::string& indexName, const Value& key);
    std::vector<uint32_t> rangeSearchByIndex(const std::string& indexName, 
                                            const Value& startKey, const Value& endKey);
    std::vector<uint32_t> searchByColumn(const std::string& tableName, 
                                        const std::string& columnName, const Value& key);
    
    // 持久化
    bool saveToStorage();
    bool loadFromStorage();
    
    // 统计和调试
    void printStorageInfo() const;
    void printTableInfo(const std::string& tableName) const;
    void printIndexInfo() const;
    void printIndexInfo(const std::string& indexName) const;
    
private:
    std::string dbPath_;
    std::unique_ptr<PageManager> pageManager_;
    std::unique_ptr<IndexManager> indexManager_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    uint32_t nextRecordId_;
    
    // 元数据管理
    bool saveMetadata();
    bool loadMetadata();
    std::string getMetadataFileName() const;
    std::string getTableFileName(const std::string& tableName) const;
};
