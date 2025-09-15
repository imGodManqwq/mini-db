#include "../../include/executor/IndexScanExecutor.h"
#include <iostream>

bool IndexScanExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    // 检查表是否存在
    tableRef_ = context_->getStorageEngine()->getTable(tableName_);
    if (!tableRef_) {
        context_->setError("Table '" + tableName_ + "' does not exist");
        return false;
    }
    
    // 使用索引查询记录ID
    try {
        if (isRangeSearch_) {
            recordIds_ = context_->getStorageEngine()->rangeSearchByIndex(indexName_, startKey_, endKey_);
        } else {
            recordIds_ = context_->getStorageEngine()->searchByIndex(indexName_, searchKey_);
        }
        
        currentIndex_ = 0;
        initialized_ = true;
        return true;
        
    } catch (const std::exception& e) {
        context_->setError("Index scan failed: " + std::string(e.what()));
        return false;
    }
}

ExecutionResult IndexScanExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    // 检查是否还有更多记录
    if (currentIndex_ >= recordIds_.size()) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        // 获取当前记录ID对应的行数据
        uint32_t recordId = recordIds_[currentIndex_];
        Row row = tableRef_->getRow(recordId);
        
        currentIndex_++;
        
        // 创建包含单行的结果
        ExecutionResult result(ExecutionResultType::SUCCESS);
        result.rows.push_back(row);
        return result;
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Failed to retrieve row: " + std::string(e.what()));
    }
}

std::vector<ColumnInfo> IndexScanExecutor::getOutputSchema() const {
    if (tableRef_) {
        return tableRef_->getColumns();
    }
    return {};
}
