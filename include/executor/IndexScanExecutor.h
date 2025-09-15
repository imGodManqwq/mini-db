#pragma once
#include "Executor.h"
#include "../storage/Row.h"
#include <memory>
#include <vector>

// 索引扫描执行算子
class IndexScanExecutor : public Executor {
public:
    explicit IndexScanExecutor(ExecutionContext* context, const std::string& tableName,
                              const std::string& indexName, const Value& key)
        : Executor(context), tableName_(tableName), indexName_(indexName), 
          searchKey_(key), currentIndex_(0) {}
    
    // 范围查询构造函数
    explicit IndexScanExecutor(ExecutionContext* context, const std::string& tableName,
                              const std::string& indexName, const Value& startKey, const Value& endKey)
        : Executor(context), tableName_(tableName), indexName_(indexName), 
          startKey_(startKey), endKey_(endKey), isRangeSearch_(true), currentIndex_(0) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // IndexScan没有子节点
    }
    
    std::string getType() const override { return "IndexScanExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override;
    
    // 获取表名和索引名
    const std::string& getTableName() const { return tableName_; }
    const std::string& getIndexName() const { return indexName_; }
    
private:
    std::string tableName_;
    std::string indexName_;
    Value searchKey_;      // 精确查询的键值
    Value startKey_;       // 范围查询的起始键值
    Value endKey_;         // 范围查询的结束键值
    bool isRangeSearch_ = false;
    
    std::shared_ptr<Table> tableRef_;    // 保持表的引用
    std::vector<uint32_t> recordIds_;    // 索引返回的记录ID列表
    size_t currentIndex_;                // 当前处理的记录索引
};
