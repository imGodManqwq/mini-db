#include "../../include/executor/SeqScanExecutor.h"
#include <iostream>

bool SeqScanExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    // 获取表
    tableRef_ = context_->getStorageEngine()->getTable(tableName_);
    if (!tableRef_) {
        context_->setError("Table '" + tableName_ + "' does not exist");
        return false;
    }
    
    // 初始化迭代器
    auto beginIter = tableRef_->begin();
    auto endIter = tableRef_->end();
    
    currentIterator_ = std::make_unique<RowIterator>(beginIter);
    endIterator_ = std::make_unique<RowIterator>(endIter);
    
    initialized_ = true;
    return true;
}

ExecutionResult SeqScanExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (!currentIterator_ || !endIterator_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Iterators not initialized");
    }
    
    if (*currentIterator_ == *endIterator_) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        // 获取当前行
        Row currentRow = **currentIterator_;
        
        // 移动到下一行
        ++(*currentIterator_);
        
        // 返回结果
        ExecutionResult result(ExecutionResultType::SUCCESS);
        result.rows.push_back(currentRow);
        result.affectedRows = 1;
        
        return result;
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during sequential scan: " + std::string(e.what()));
    }
}

std::vector<ColumnInfo> SeqScanExecutor::getOutputSchema() const {
    if (!tableRef_) {
        return {};
    }
    
    return tableRef_->getColumns();
}
