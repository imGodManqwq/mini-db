#include "../../include/executor/DeleteExecutor.h"
#include <iostream>

bool DeleteExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!deleteStmt_) {
        context_->setError("DeleteStatement is null");
        return false;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    // 检查表是否存在
    if (!context_->getStorageEngine()->tableExists(deleteStmt_->tableName)) {
        context_->setError("Table '" + deleteStmt_->tableName + "' does not exist");
        return false;
    }
    
    initialized_ = true;
    finished_ = false;
    return true;
}

ExecutionResult DeleteExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (finished_) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        auto table = context_->getStorageEngine()->getTable(deleteStmt_->tableName);
        if (!table) {
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Cannot get table '" + deleteStmt_->tableName + "'");
        }
        
        size_t deletedCount = 0;
        size_t failedCount = 0;
        std::string errorMessages;
        
        // 使用两阶段删除策略：先收集要删除的recordId，然后批量删除
        std::vector<uint32_t> allRecordIds = table->getAllRecordIds();
        std::vector<std::pair<uint32_t, Row>> recordsToDelete;
        
        // 第一阶段：收集所有需要删除的记录
        for (uint32_t recordId : allRecordIds) {
            Row currentRow = table->getRow(recordId);
            if (currentRow.getFieldCount() == 0) {
                continue;
            }
            
            // 检查WHERE条件
            bool shouldDelete = true;
            if (deleteStmt_->whereClause) {
                shouldDelete = evaluateWhereCondition(deleteStmt_->whereClause.get(), currentRow);
            }
            
            if (shouldDelete) {
                recordsToDelete.push_back({recordId, currentRow});
            }
        }
        
        // 第二阶段：执行删除操作
        for (const auto& recordPair : recordsToDelete) {
            uint32_t recordId = recordPair.first;
            Row rowCopy = recordPair.second;
            
            bool success = context_->getStorageEngine()->deleteRow(deleteStmt_->tableName, rowCopy, recordId);
            if (success) {
                deletedCount++;
            } else {
                failedCount++;
                if (!errorMessages.empty()) {
                    errorMessages += "; ";
                }
                errorMessages += "Failed to delete record " + std::to_string(recordId);
            }
        }
        
        finished_ = true;
        
        // 根据结果决定返回成功还是失败
        if (failedCount == 0) {
            ExecutionResult result(ExecutionResultType::SUCCESS, 
                                 "Deleted " + std::to_string(deletedCount) + " rows");
            result.affectedRows = deletedCount;
            return result;
        } else if (deletedCount > 0) {
            // 部分成功
            ExecutionResult result(ExecutionResultType::SUCCESS, 
                                 "Deleted " + std::to_string(deletedCount) + " rows, " + 
                                 std::to_string(failedCount) + " failed: " + errorMessages);
            result.affectedRows = deletedCount;
            return result;
        } else {
            // 全部失败
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "All deletes failed: " + errorMessages);
        }
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during DELETE: " + std::string(e.what()));
    }
}

Value DeleteExecutor::evaluateExpression(Expression* expr, const Row& currentRow) {
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
            auto table = context_->getStorageEngine()->getTable(deleteStmt_->tableName);
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
            
            // 实现基本的比较运算
            switch (binary->operator_) {
                case TokenType::EQUAL:
                    {
                        // 处理数值比较
                        if ((std::holds_alternative<int>(left) || std::holds_alternative<double>(left)) &&
                            (std::holds_alternative<int>(right) || std::holds_alternative<double>(right))) {
                            double leftVal = std::holds_alternative<int>(left) ? 
                                static_cast<double>(std::get<int>(left)) : std::get<double>(left);
                            double rightVal = std::holds_alternative<int>(right) ? 
                                static_cast<double>(std::get<int>(right)) : std::get<double>(right);
                            return (std::abs(leftVal - rightVal) < 1e-9) ? 1 : 0;
                        }
                        // 处理字符串和其他类型的直接比较
                        return (left == right) ? 1 : 0;
                    }
                    
                case TokenType::NOT_EQUAL:
                    {
                        // 处理数值比较
                        if ((std::holds_alternative<int>(left) || std::holds_alternative<double>(left)) &&
                            (std::holds_alternative<int>(right) || std::holds_alternative<double>(right))) {
                            double leftVal = std::holds_alternative<int>(left) ? 
                                static_cast<double>(std::get<int>(left)) : std::get<double>(left);
                            double rightVal = std::holds_alternative<int>(right) ? 
                                static_cast<double>(std::get<int>(right)) : std::get<double>(right);
                            return (std::abs(leftVal - rightVal) >= 1e-9) ? 1 : 0;
                        }
                        // 处理字符串和其他类型的直接比较
                        return (left != right) ? 1 : 0;
                    }
                    
                case TokenType::GREATER_THAN:
                    {
                        double leftVal = 0.0, rightVal = 0.0;
                        if (std::holds_alternative<int>(left)) leftVal = static_cast<double>(std::get<int>(left));
                        else if (std::holds_alternative<double>(left)) leftVal = std::get<double>(left);
                        else break;
                        
                        if (std::holds_alternative<int>(right)) rightVal = static_cast<double>(std::get<int>(right));
                        else if (std::holds_alternative<double>(right)) rightVal = std::get<double>(right);
                        else break;
                        
                        return (leftVal > rightVal) ? 1 : 0;
                    }
                    
                case TokenType::GREATER_EQUAL:
                    {
                        double leftVal = 0.0, rightVal = 0.0;
                        if (std::holds_alternative<int>(left)) leftVal = static_cast<double>(std::get<int>(left));
                        else if (std::holds_alternative<double>(left)) leftVal = std::get<double>(left);
                        else break;
                        
                        if (std::holds_alternative<int>(right)) rightVal = static_cast<double>(std::get<int>(right));
                        else if (std::holds_alternative<double>(right)) rightVal = std::get<double>(right);
                        else break;
                        
                        return (leftVal >= rightVal) ? 1 : 0;
                    }
                    
                case TokenType::LESS_THAN:
                    {
                        double leftVal = 0.0, rightVal = 0.0;
                        if (std::holds_alternative<int>(left)) leftVal = static_cast<double>(std::get<int>(left));
                        else if (std::holds_alternative<double>(left)) leftVal = std::get<double>(left);
                        else break;
                        
                        if (std::holds_alternative<int>(right)) rightVal = static_cast<double>(std::get<int>(right));
                        else if (std::holds_alternative<double>(right)) rightVal = std::get<double>(right);
                        else break;
                        
                        // Comparison: leftVal < rightVal
                        return (leftVal < rightVal) ? 1 : 0;
                    }
                    
                case TokenType::LESS_EQUAL:
                    {
                        double leftVal = 0.0, rightVal = 0.0;
                        if (std::holds_alternative<int>(left)) leftVal = static_cast<double>(std::get<int>(left));
                        else if (std::holds_alternative<double>(left)) leftVal = std::get<double>(left);
                        else break;
                        
                        if (std::holds_alternative<int>(right)) rightVal = static_cast<double>(std::get<int>(right));
                        else if (std::holds_alternative<double>(right)) rightVal = std::get<double>(right);
                        else break;
                        
                        return (leftVal <= rightVal) ? 1 : 0;
                    }
                    
                case TokenType::AND:
                    {
                        int leftVal = 0, rightVal = 0;
                        if (std::holds_alternative<int>(left)) leftVal = std::get<int>(left);
                        if (std::holds_alternative<int>(right)) rightVal = std::get<int>(right);
                        return (leftVal && rightVal) ? 1 : 0;
                    }
                    
                case TokenType::OR:
                    {
                        int leftVal = 0, rightVal = 0;
                        if (std::holds_alternative<int>(left)) leftVal = std::get<int>(left);
                        if (std::holds_alternative<int>(right)) rightVal = std::get<int>(right);
                        return (leftVal || rightVal) ? 1 : 0;
                    }
                    
                default:
                    break;
            }
            
            throw std::runtime_error("Unsupported binary operation in DELETE");
        }
        
        default:
            throw std::runtime_error("Unsupported expression type in DELETE");
    }
}

bool DeleteExecutor::evaluateWhereCondition(Expression* whereExpr, const Row& row) {
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
