#include "../../include/executor/ProjectExecutor.h"
#include <iostream>

bool ProjectExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (children_.empty() || !children_[0]) {
        context_->setError("ProjectExecutor requires a child executor");
        return false;
    }
    
    if (projections_.empty()) {
        context_->setError("ProjectExecutor requires at least one projection");
        return false;
    }
    
    // 初始化子执行器
    if (!children_[0]->init()) {
        context_->setError("Failed to initialize child executor");
        return false;
    }
    
    // 构建输出模式
    auto inputSchema = children_[0]->getOutputSchema();
    outputSchema_.clear();
    
    for (auto* projection : projections_) {
        if (!projection) {
            context_->setError("Null projection expression");
            return false;
        }
        
        // 处理 SELECT * 的情况
        if (auto* identifier = dynamic_cast<IdentifierExpression*>(projection)) {
            if (identifier->name == "*") {
                // 添加所有列
                for (const auto& col : inputSchema) {
                    outputSchema_.push_back(col);
                }
                continue;
            }
        }
        
        // 推断表达式类型和名称
        std::string columnName = getExpressionName(projection);
        DataType columnType = inferExpressionType(projection, inputSchema);
        
        outputSchema_.emplace_back(columnName, columnType);
    }
    
    initialized_ = true;
    return true;
}

ExecutionResult ProjectExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    try {
        // 从子执行器获取下一行
        auto childResult = children_[0]->next();
        
        if (childResult.isError()) {
            return childResult;
        }
        
        if (childResult.isEndOfData()) {
            return ExecutionResult(ExecutionResultType::END_OF_DATA);
        }
        
        auto inputSchema = children_[0]->getOutputSchema();
        ExecutionResult result(ExecutionResultType::SUCCESS);
        
        // 对每一行应用投影
        for (const auto& inputRow : childResult.rows) {
            std::vector<Value> projectedValues;
            
            for (auto* projection : projections_) {
                // 处理 SELECT * 的情况
                if (auto* identifier = dynamic_cast<IdentifierExpression*>(projection)) {
                    if (identifier->name == "*") {
                        // 添加所有列的值
                        for (size_t i = 0; i < inputRow.getFieldCount(); ++i) {
                            projectedValues.push_back(inputRow.getValue(i));
                        }
                        continue;
                    }
                }
                
                // 计算投影表达式
                Value projectedValue = evaluateExpression(projection, inputRow, inputSchema);
                projectedValues.push_back(projectedValue);
            }
            
            Row projectedRow(projectedValues);
            result.rows.push_back(projectedRow);
        }
        
        result.affectedRows = result.rows.size();
        return result;
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during projection: " + std::string(e.what()));
    }
}

std::vector<ColumnInfo> ProjectExecutor::getOutputSchema() const {
    return outputSchema_;
}

Value ProjectExecutor::evaluateExpression(Expression* expr, const Row& row, const std::vector<ColumnInfo>& inputSchema) {
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
            int columnIndex = findColumnIndex(identifier->name, inputSchema);
            if (columnIndex < 0) {
                throw std::runtime_error("Column '" + identifier->name + "' not found");
            }
            return row.getValue(columnIndex);
        }
        
        case ASTNodeType::BINARY_EXPR: {
            auto* binary = static_cast<BinaryExpression*>(expr);
            Value left = evaluateExpression(binary->left.get(), row, inputSchema);
            Value right = evaluateExpression(binary->right.get(), row, inputSchema);
            
            // 算术操作
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
            
            if (binary->operator_ == TokenType::MINUS) {
                if (std::holds_alternative<int>(left) && std::holds_alternative<int>(right)) {
                    return std::get<int>(left) - std::get<int>(right);
                }
                if (std::holds_alternative<double>(left) && std::holds_alternative<double>(right)) {
                    return std::get<double>(left) - std::get<double>(right);
                }
                if (std::holds_alternative<int>(left) && std::holds_alternative<double>(right)) {
                    return static_cast<double>(std::get<int>(left)) - std::get<double>(right);
                }
                if (std::holds_alternative<double>(left) && std::holds_alternative<int>(right)) {
                    return std::get<double>(left) - static_cast<double>(std::get<int>(right));
                }
            }
            
            throw std::runtime_error("Unsupported binary operation in projection");
        }
        
        default:
            throw std::runtime_error("Unsupported expression type in projection");
    }
}

int ProjectExecutor::findColumnIndex(const std::string& columnName, const std::vector<ColumnInfo>& schema) {
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].name == columnName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

std::string ProjectExecutor::getExpressionName(Expression* expr) {
    if (!expr) {
        return "unknown";
    }
    
    switch (expr->nodeType) {
        case ASTNodeType::IDENTIFIER_EXPR: {
            auto* identifier = static_cast<IdentifierExpression*>(expr);
            return identifier->name;
        }
        
        case ASTNodeType::LITERAL_EXPR: {
            auto* literal = static_cast<LiteralExpression*>(expr);
            if (std::holds_alternative<int>(literal->value)) {
                return std::to_string(std::get<int>(literal->value));
            }
            if (std::holds_alternative<double>(literal->value)) {
                return std::to_string(std::get<double>(literal->value));
            }
            if (std::holds_alternative<std::string>(literal->value)) {
                return "'" + std::get<std::string>(literal->value) + "'";
            }
            return "literal";
        }
        
        case ASTNodeType::BINARY_EXPR: {
            auto* binary = static_cast<BinaryExpression*>(expr);
            return getExpressionName(binary->left.get()) + "_op_" + getExpressionName(binary->right.get());
        }
        
        default:
            return "expr";
    }
}

DataType ProjectExecutor::inferExpressionType(Expression* expr, const std::vector<ColumnInfo>& inputSchema) {
    if (!expr) {
        return DataType::INT; // 默认类型
    }
    
    switch (expr->nodeType) {
        case ASTNodeType::LITERAL_EXPR: {
            auto* literal = static_cast<LiteralExpression*>(expr);
            if (std::holds_alternative<int>(literal->value)) {
                return DataType::INT;
            }
            if (std::holds_alternative<double>(literal->value)) {
                return DataType::DOUBLE;
            }
            if (std::holds_alternative<std::string>(literal->value)) {
                return DataType::STRING;
            }
            return DataType::INT;
        }
        
        case ASTNodeType::IDENTIFIER_EXPR: {
            auto* identifier = static_cast<IdentifierExpression*>(expr);
            int columnIndex = findColumnIndex(identifier->name, inputSchema);
            if (columnIndex >= 0) {
                return inputSchema[columnIndex].type;
            }
            return DataType::INT;
        }
        
        case ASTNodeType::BINARY_EXPR: {
            auto* binary = static_cast<BinaryExpression*>(expr);
            DataType leftType = inferExpressionType(binary->left.get(), inputSchema);
            DataType rightType = inferExpressionType(binary->right.get(), inputSchema);
            
            // 算术运算的结果类型
            if (binary->operator_ == TokenType::PLUS || 
                binary->operator_ == TokenType::MINUS ||
                binary->operator_ == TokenType::MULTIPLY ||
                binary->operator_ == TokenType::DIVIDE) {
                
                if (leftType == DataType::DOUBLE || rightType == DataType::DOUBLE) {
                    return DataType::DOUBLE;
                }
                return DataType::INT;
            }
            
            // 比较运算的结果是布尔值（用INT表示）
            return DataType::INT;
        }
        
        default:
            return DataType::INT;
    }
}
