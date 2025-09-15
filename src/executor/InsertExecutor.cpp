#include "../../include/executor/InsertExecutor.h"
#include <iostream>

bool InsertExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (!insertStmt_) {
        context_->setError("InsertStatement is null");
        return false;
    }
    
    if (!context_->getStorageEngine()) {
        context_->setError("StorageEngine is null");
        return false;
    }
    
    // 检查表是否存在
    if (!context_->getStorageEngine()->tableExists(insertStmt_->tableName)) {
        context_->setError("Table '" + insertStmt_->tableName + "' does not exist");
        return false;
    }
    
    initialized_ = true;
    currentValueIndex_ = 0;
    return true;
}

ExecutionResult InsertExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    if (currentValueIndex_ >= insertStmt_->valuesList.size()) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    try {
        // 获取当前要插入的值列表
        const auto& currentValues = insertStmt_->valuesList[currentValueIndex_];
        
        // 计算表达式得到实际值
        std::vector<Value> values = evaluateValueList(currentValues);
        
        // 插入到存储引擎
        bool success = context_->getStorageEngine()->insertRow(insertStmt_->tableName, values);
        
        currentValueIndex_++;
        
        if (success) {
            ExecutionResult result(ExecutionResultType::SUCCESS, "Row inserted successfully");
            result.affectedRows = 1;
            return result;
        } else {
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Failed to insert row into table '" + insertStmt_->tableName + "'");
        }
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during row insertion: " + std::string(e.what()));
    }
}

Value InsertExecutor::evaluateExpression(Expression* expr) {
    if (!expr) {
        throw std::runtime_error("Expression is null");
    }
    
    switch (expr->nodeType) {
        case ASTNodeType::LITERAL_EXPR: {
            auto* literal = static_cast<LiteralExpression*>(expr);
            return literal->value;
        }
        
        case ASTNodeType::IDENTIFIER_EXPR: {
            // 在INSERT语句中，标识符通常不应该出现（除非是DEFAULT等特殊情况）
            throw std::runtime_error("Identifier expressions not supported in INSERT VALUES");
        }
        
        case ASTNodeType::BINARY_EXPR: {
            auto* binary = static_cast<BinaryExpression*>(expr);
            Value left = evaluateExpression(binary->left.get());
            Value right = evaluateExpression(binary->right.get());
            
            // 简单的算术运算支持
            if (binary->operator_ == TokenType::PLUS) {
                if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                    return std::get<int>(left) + std::get<int>(right);
                }
                if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                    return std::get<double>(left) + std::get<double>(right);
                }
                if (std::holds_alternative<int>(left) && std::holds_alternative<double>(right)) {
                    return static_cast<double>(std::get<int>(left)) + std::get<double>(right);
                }
                if (std::holds_alternative<double>(left) && std::holds_alternative<int>(right)) {
                    return std::get<double>(left) + static_cast<double>(std::get<int>(right));
                }
            }
            
            throw std::runtime_error("Unsupported binary operation in INSERT VALUES");
        }
        
        default:
            throw std::runtime_error("Unsupported expression type in INSERT VALUES");
    }
}

std::vector<Value> InsertExecutor::evaluateValueList(const std::vector<std::unique_ptr<Expression>>& expressions) {
    std::vector<Value> values;
    values.reserve(expressions.size());
    
    for (const auto& expr : expressions) {
        values.push_back(evaluateExpression(expr.get()));
    }
    
    return values;
}
