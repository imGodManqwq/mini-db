#include "../../include/executor/CreateIndexExecutor.h"
#include <iostream>

bool CreateIndexExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!createIndexStmt_) {
        context_->setError("CreateIndexStatement is null");
        return false;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    initialized_ = true;
    executed_ = false;
    return true;
}

ExecutionResult CreateIndexExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (executed_) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        // 创建索引
        bool success = context_->getStorageEngine()->createIndex(
            createIndexStmt_->indexName,
            createIndexStmt_->tableName,
            createIndexStmt_->columnName,
            createIndexStmt_->isUnique
        );
        
        executed_ = true;
        
        if (success) {
            std::string message = (createIndexStmt_->isUnique ? "Unique index '" : "Index '") +
                                createIndexStmt_->indexName + "' created successfully";
            ExecutionResult result(ExecutionResultType::SUCCESS, message);
            result.affectedRows = 1; // 表示创建了1个索引
            return result;
        } else {
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Failed to create index '" + createIndexStmt_->indexName + "'");
        }
        
    } catch (const std::exception& e) {
        executed_ = true;
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during index creation: " + std::string(e.what()));
    }
}
