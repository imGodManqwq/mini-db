#include "../../include/executor/DropTableExecutor.h"
#include <iostream>

bool DropTableExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!dropStmt_) {
        context_->setError("DropTableStatement is null");
        return false;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    initialized_ = true;
    finished_ = false;
    return true;
}

ExecutionResult DropTableExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (finished_) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        // 检查表是否存在
        bool tableExists = context_->getStorageEngine()->tableExists(dropStmt_->tableName);
        
        if (!tableExists) {
            if (dropStmt_->ifExists) {
                // IF EXISTS 子句，表不存在时不报错
                finished_ = true;
                return ExecutionResult(ExecutionResultType::SUCCESS, 
                                     "Table '" + dropStmt_->tableName + "' does not exist (IF EXISTS used)");
            } else {
                // 表不存在且没有 IF EXISTS 子句，报错
                return ExecutionResult(ExecutionResultType::ERROR, 
                                     "Table '" + dropStmt_->tableName + "' does not exist");
            }
        }
        
        // 删除表
        bool success = context_->getStorageEngine()->dropTable(dropStmt_->tableName);
        
        finished_ = true;
        
        if (success) {
            return ExecutionResult(ExecutionResultType::SUCCESS, 
                                 "Table '" + dropStmt_->tableName + "' dropped successfully");
        } else {
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Failed to drop table '" + dropStmt_->tableName + "'");
        }
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during DROP TABLE: " + std::string(e.what()));
    }
}
