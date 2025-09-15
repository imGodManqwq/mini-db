#include "../../include/executor/CreateTableExecutor.h"
#include <iostream>

bool CreateTableExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!createStmt_) {
        context_->setError("CreateTableStatement is null");
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

ExecutionResult CreateTableExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (executed_) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        // 构建列信息
        std::vector<ColumnInfo> columns;
        for (const auto& colDef : createStmt_->columns) {
            columns.emplace_back(colDef->columnName, colDef->dataType, 
                               colDef->isNotNull, colDef->isPrimaryKey);
        }
        
        // 创建表
        bool success = context_->getStorageEngine()->createTable(createStmt_->tableName, columns);
        
        executed_ = true;
        
        if (success) {
            ExecutionResult result(ExecutionResultType::SUCCESS, 
                                 "Table '" + createStmt_->tableName + "' created successfully");
            result.affectedRows = 1; // 表示创建了1个表
            return result;
        } else {
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Failed to create table '" + createStmt_->tableName + "'");
        }
        
    } catch (const std::exception& e) {
        executed_ = true;
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during table creation: " + std::string(e.what()));
    }
}
