#include "../../include/executor/UpdateExecutor.h"
#include <iostream>

bool UpdateExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!updateStmt_) {
        context_->setError("UpdateStatement is null");
        return false;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    // 检查表是否存在
    if (!context_->getStorageEngine()->tableExists(updateStmt_->tableName)) {
        context_->setError("Table '" + updateStmt_->tableName + "' does not exist");
        return false;
    }
    
    initialized_ = true;
    finished_ = false;
    return true;
}

ExecutionResult UpdateExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (finished_) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        auto table = context_->getStorageEngine()->getTable(updateStmt_->tableName);
        if (!table) {
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Cannot get table '" + updateStmt_->tableName + "'");
        }
        
        size_t updatedCount = 0;
        size_t failedCount = 0;
        std::string errorMessages;
        
        // 使用基于PRIMARY KEY的更新策略来避免recordId变化问题
        // 如果表有PRIMARY KEY，使用PRIMARY KEY来标识记录；否则使用recordId
        
        // 获取PRIMARY KEY列索引
        int pkIndex = -1;
        for (size_t i = 0; i < table->getColumns().size(); ++i) {
            if (table->getColumns()[i].isPrimaryKey) {
                pkIndex = static_cast<int>(i);
                break;
            }
        }
        
        if (pkIndex >= 0) {
            // 使用PRIMARY KEY策略：先收集所有需要更新的PRIMARY KEY值
            std::vector<Value> keysToUpdate;
            std::vector<uint32_t> allRecordIds = table->getAllRecordIds();
            
            for (uint32_t recordId : allRecordIds) {
                Row currentRow = table->getRow(recordId);
                if (currentRow.getFieldCount() == 0) {
                    continue;
                }
                
                // 检查WHERE条件
                bool shouldUpdate = true;
                if (updateStmt_->whereClause) {
                    shouldUpdate = evaluateWhereCondition(updateStmt_->whereClause.get(), currentRow);
                }
                
                if (shouldUpdate) {
                    keysToUpdate.push_back(currentRow.getValue(pkIndex));
                }
            }
            
            // 根据PRIMARY KEY值进行更新
            for (const Value& pkValue : keysToUpdate) {
                // 通过PRIMARY KEY查找当前记录
                std::vector<uint32_t> currentRecordIds = table->getAllRecordIds();
                for (uint32_t recordId : currentRecordIds) {
                    Row currentRow = table->getRow(recordId);
                    if (currentRow.getFieldCount() == 0) {
                        continue;
                    }
                    
                    // 检查是否是目标记录
                    if (currentRow.getValue(pkIndex) == pkValue) {
                        // 立即更新这条记录
                        Row oldRow = currentRow;
                        
                        // 创建新行，保持原有列的顺序，只更新指定的列
                        std::vector<Value> allValues;
                        
                        // 先复制所有原始值
                        for (size_t j = 0; j < oldRow.getFieldCount(); ++j) {
                            allValues.push_back(oldRow.getValue(j));
                        }
                        
                        // 然后更新指定的列
                        for (const auto& assignment : updateStmt_->assignments) {
                            int columnIndex = table->getColumnIndex(assignment->columnName);
                            
                            if (columnIndex >= 0 && columnIndex < static_cast<int>(allValues.size())) {
                                Value newValue = evaluateExpression(assignment->value.get(), oldRow);
                                allValues[columnIndex] = newValue;
                            }
                        }
                        
                        Row newRow(allValues);
                        
                        // 执行更新操作
                        bool success = context_->getStorageEngine()->updateRow(updateStmt_->tableName, oldRow, newRow, recordId);
                        if (success) {
                            updatedCount++;
                        } else {
                            failedCount++;
                            if (!errorMessages.empty()) {
                                errorMessages += "; ";
                            }
                            errorMessages += "Failed to update record " + std::to_string(recordId);
                        }
                        break; // 找到并更新了目标记录，退出内层循环
                    }
                }
            }
        } else {
            // 没有PRIMARY KEY的情况，使用原来的recordId策略
            std::vector<uint32_t> allRecordIds = table->getAllRecordIds();
            
            for (uint32_t recordId : allRecordIds) {
                Row currentRow = table->getRow(recordId);
                if (currentRow.getFieldCount() == 0) {
                    continue;
                }
                
                // 检查WHERE条件
                bool shouldUpdate = true;
                if (updateStmt_->whereClause) {
                    shouldUpdate = evaluateWhereCondition(updateStmt_->whereClause.get(), currentRow);
                }
                
                if (!shouldUpdate) {
                    continue;
                }
                
                // 立即更新这条记录
                Row oldRow = currentRow;
                
                // 创建新行，保持原有列的顺序，只更新指定的列
                std::vector<Value> allValues;
                
                // 先复制所有原始值
                for (size_t j = 0; j < oldRow.getFieldCount(); ++j) {
                    allValues.push_back(oldRow.getValue(j));
                }
                
                // 然后更新指定的列
                for (const auto& assignment : updateStmt_->assignments) {
                    int columnIndex = table->getColumnIndex(assignment->columnName);
                    
                    if (columnIndex >= 0 && columnIndex < static_cast<int>(allValues.size())) {
                        Value newValue = evaluateExpression(assignment->value.get(), oldRow);
                        allValues[columnIndex] = newValue;
                    }
                }
                
                Row newRow(allValues);
                
                // 执行更新操作
                bool success = context_->getStorageEngine()->updateRow(updateStmt_->tableName, oldRow, newRow, recordId);
                if (success) {
                    updatedCount++;
                } else {
                    failedCount++;
                    if (!errorMessages.empty()) {
                        errorMessages += "; ";
                    }
                    errorMessages += "Failed to update record " + std::to_string(recordId);
                }
            }
        }
        
        finished_ = true;
        
        // 根据结果决定返回成功还是失败
        if (failedCount == 0) {
            ExecutionResult result(ExecutionResultType::SUCCESS, 
                                 "Updated " + std::to_string(updatedCount) + " rows");
            result.affectedRows = updatedCount;
            return result;
        } else if (updatedCount > 0) {
            // 部分成功
            ExecutionResult result(ExecutionResultType::SUCCESS, 
                                 "Updated " + std::to_string(updatedCount) + " rows, " + 
                                 std::to_string(failedCount) + " failed: " + errorMessages);
            result.affectedRows = updatedCount;
            return result;
        } else {
            // 全部失败
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "All updates failed: " + errorMessages);
        }
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during UPDATE: " + std::string(e.what()));
    }
}

Value UpdateExecutor::evaluateExpression(Expression* expr, const Row& currentRow) {
    if (!expr) {
        throw std::runtime_error("Expression is null");
    }
    
    switch (expr->nodeType) {
        case ASTNodeType::LITERAL_EXPR: {
            auto* literal = static_cast<LiteralExpression*>(expr);
            return literal->value;
        }
        
        case ASTNodeType::IDENTIFIER_EXPR: {
            auto* identifier = static_cast<IdentifierExpression*>(expr);
            auto table = context_->getStorageEngine()->getTable(updateStmt_->tableName);
            if (!table) {
                throw std::runtime_error("Cannot get table for identifier evaluation");
            }
            
            int columnIndex = table->getColumnIndex(identifier->name);
            if (columnIndex < 0) {
                throw std::runtime_error("Column '" + identifier->name + "' not found");
            }
            
            return currentRow.getValue(columnIndex);
        }
        
        case ASTNodeType::BINARY_EXPR: {
            auto* binary = static_cast<BinaryExpression*>(expr);
            Value left = evaluateExpression(binary->left.get(), currentRow);
            Value right = evaluateExpression(binary->right.get(), currentRow);
            
            // 实现基本的算术和比较运算
            switch (binary->operator_) {
                case TokenType::PLUS:
                    if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                        return std::get<int>(left) + std::get<int>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) + std::get<double>(right);
                    }
                    break;
                    
                case TokenType::MINUS:
                    if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                        return std::get<int>(left) - std::get<int>(right);
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return std::get<double>(left) - std::get<double>(right);
                    }
                    break;
                    
                case TokenType::EQUAL:
                    return (left == right) ? 1 : 0;
                    
                case TokenType::NOT_EQUAL:
                    return (left != right) ? 1 : 0;
                    
                case TokenType::GREATER_THAN:
                    if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                        return (std::get<int>(left) > std::get<int>(right)) ? 1 : 0;
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return (std::get<double>(left) > std::get<double>(right)) ? 1 : 0;
                    }
                    break;
                    
                case TokenType::GREATER_EQUAL:
                    if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                        return (std::get<int>(left) >= std::get<int>(right)) ? 1 : 0;
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return (std::get<double>(left) >= std::get<double>(right)) ? 1 : 0;
                    }
                    break;
                    
                case TokenType::LESS_THAN:
                    if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                        return (std::get<int>(left) < std::get<int>(right)) ? 1 : 0;
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return (std::get<double>(left) < std::get<double>(right)) ? 1 : 0;
                    }
                    break;
                    
                case TokenType::LESS_EQUAL:
                    if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                        return (std::get<int>(left) <= std::get<int>(right)) ? 1 : 0;
                    }
                    if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                        return (std::get<double>(left) <= std::get<double>(right)) ? 1 : 0;
                    }
                    break;
                    
                default:
                    break;
            }
            
            throw std::runtime_error("Unsupported binary operation in UPDATE");
        }
        
        default:
            throw std::runtime_error("Unsupported expression type in UPDATE");
    }
}

bool UpdateExecutor::evaluateWhereCondition(Expression* whereExpr, const Row& row) {
    if (!whereExpr) {
        return true; // 没有WHERE条件，所有行都符合
    }
    
    try {
        Value result = evaluateExpression(whereExpr, row);
        
        // 将结果转换为布尔值
        if (std::holds_alternative<int>(result)) {
            return std::get<int>(result) != 0;
        }
        
        return false; // 默认返回false
    } catch (const std::exception&) {
        return false; // 出错时返回false
    }
}

std::vector<Value> UpdateExecutor::evaluateAssignments(const Row& currentRow) {
    std::vector<Value> values;
    values.reserve(updateStmt_->assignments.size());
    
    for (const auto& assignment : updateStmt_->assignments) {
        Value newValue = evaluateExpression(assignment->value.get(), currentRow);
        values.push_back(newValue);
    }
    
    return values;
}
