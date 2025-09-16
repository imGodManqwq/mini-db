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
        
        // 获取所有记录ID并立即删除符合条件的记录
        // 注意：我们需要在循环中重新获取记录ID，因为删除操作可能会影响ID分配
        bool continueDeleting = true;
        while (continueDeleting) {
            continueDeleting = false;
            std::vector<uint32_t> currentRecordIds = table->getAllRecordIds();
            
            for (uint32_t recordId : currentRecordIds) {
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
                    // 先保存当前行的副本，因为删除操作可能会修改行数据
                    Row rowCopy = currentRow;
                    bool success = context_->getStorageEngine()->deleteRow(deleteStmt_->tableName, rowCopy, recordId);
                    if (success) {
                        deletedCount++;
                        continueDeleting = true; // 继续查找更多需要删除的记录
                        break; // 跳出内层循环，重新获取记录ID
                    } else {
                        failedCount++;
                        if (!errorMessages.empty()) {
                            errorMessages += "; ";
                        }
                        errorMessages += "Failed to delete record " + std::to_string(recordId);
                    }
                }
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
