#pragma once
#include "BPlusTree.h"
#include "Table.h"
#include <unordered_map>
#include <string>
#include <memory>

// 索引类型
enum class IndexType {
    BTREE,      // B+树索引
    HASH        // 哈希索引（未实现）
};

// 索引信息结构
struct IndexInfo {
    std::string indexName;
    std::string tableName;
    std::string columnName;
    IndexType indexType;
    bool isUnique;
    
    IndexInfo(const std::string& name, const std::string& table, 
              const std::string& column, IndexType type, bool unique = false)
        : indexName(name), tableName(table), columnName(column), 
          indexType(type), isUnique(unique) {}
};

class IndexManager {
public:
    IndexManager() = default;
    ~IndexManager() = default;
    
    // 索引创建和删除
    bool createIndex(const std::string& indexName, const std::string& tableName,
                    const std::string& columnName, IndexType type = IndexType::BTREE,
                    bool isUnique = false);
    bool dropIndex(const std::string& indexName);
    
    // 索引操作
    bool insertRecord(const std::string& tableName, const Row& row, uint32_t recordId);
    bool deleteRecord(const std::string& tableName, const Row& row, uint32_t recordId);
    bool updateRecord(const std::string& tableName, const Row& oldRow, 
                     const Row& newRow, uint32_t recordId);
    
    // 查询操作
    std::vector<uint32_t> searchByIndex(const std::string& indexName, const Value& key) const;
    std::vector<uint32_t> rangeSearchByIndex(const std::string& indexName, 
                                            const Value& startKey, const Value& endKey) const;
    
    // 索引信息
    bool hasIndex(const std::string& tableName, const std::string& columnName) const;
    std::vector<std::string> getIndexesForTable(const std::string& tableName) const;
    const IndexInfo* getIndexInfo(const std::string& indexName) const;
    
    // 表操作
    void registerTable(std::shared_ptr<Table> table);
    void unregisterTable(const std::string& tableName);
    
<<<<<<< HEAD
=======
    // 持久化
    bool saveIndexes(const std::string& dbPath) const;
    bool loadIndexes(const std::string& dbPath);
    void rebuildIndexes(); // 重建索引（加载后调用）
    
>>>>>>> origin/storage
    // 调试和统计
    void printIndexStats() const;
    void printIndexInfo(const std::string& indexName) const;
    
private:
    std::unordered_map<std::string, std::unique_ptr<BPlusTree>> indexes_;
    std::unordered_map<std::string, std::unique_ptr<IndexInfo>> indexInfos_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    
    // 辅助函数
    std::string generateIndexKey(const std::string& tableName, const std::string& columnName) const;
    Value extractColumnValue(const Row& row, const std::string& tableName, 
                           const std::string& columnName) const;
    bool validateIndexName(const std::string& indexName) const;
};
