#pragma once
#include "Executor.h"
#include "../storage/RowIterator.h"
#include <memory>

// 顺序扫描执行算子
class SeqScanExecutor : public Executor {
public:
    explicit SeqScanExecutor(ExecutionContext* context, const std::string& tableName)
        : Executor(context), tableName_(tableName), tableRef_(nullptr), 
          currentIterator_(nullptr), endIterator_(nullptr) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // SeqScan没有子节点
    }
    
    std::string getType() const override { return "SeqScanExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override;
    
    // 获取表名
    const std::string& getTableName() const { return tableName_; }
    
private:
    std::string tableName_;
    std::shared_ptr<Table> tableRef_; // 保持表的引用
    std::unique_ptr<RowIterator> currentIterator_;
    std::unique_ptr<RowIterator> endIterator_;
};
