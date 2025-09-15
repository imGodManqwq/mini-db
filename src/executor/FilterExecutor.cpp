#include "../../include/executor/FilterExecutor.h"
#include <iostream>

bool FilterExecutor::init() {
    if (initialized_) {
        return true;
    }
    
    if (children_.empty() || !children_[0]) {
        context_->setError("FilterExecutor requires a child executor");
        return false;
    }
    
    if (!predicate_) {
        context_->setError("FilterExecutor requires a predicate");
        return false;
    }
    
    // 初始化子执行器
    if (!children_[0]->init()) {
        context_->setError("Failed to initialize child executor");
        return false;
    }
    
    initialized_ = true;
    return true;
}

ExecutionResult FilterExecutor::next() {
    if (!initialized_) {
        return ExecutionResult(ExecutionResultType::ERROR, "Executor not initialized");
    }
    
    try {
        auto schema = getOutputSchema();
        
        while (true) {
            // 从子执行器获取下一行
            auto childResult = children_[0]->next();
            
            if (childResult.isError()) {
                return childResult;
            }
            
            if (childResult.isEndOfData()) {
                return ExecutionResult(ExecutionResultType::END_OF_DATA);
            }
            
            // 对每一行应用谓词
            for (const auto& row : childResult.rows) {
                if (evaluatePredicate(row, schema)) {
                    ExecutionResult result(ExecutionResultType::SUCCESS);
                    result.rows.push_back(row);
                    result.affectedRows = 1;
                    return result;
                }
            }
        }
        
    } catch (const std::exception& e) {
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during filtering: " + std::string(e.what()));
    }
}

bool FilterExecutor::evaluatePredicate(const Row& row, const std::vector<ColumnInfo>& schema) {
    try {
        Value result = evaluateExpression(predicate_, row, schema);
        
        // 将结果转换为布尔值
        if (std::holds_alternative<int>(result)) {
            return std::get<int>(result) != 0;
        }
        if (std::holds_alternative<double>(result)) {
            return std::get<double>(result) != 0.0;
        }
        if (std::holds_alternative<std::string>(result)) {
            return !std::get<std::string>(result).empty();
        }
        
        return false;
        
    } catch (const std::exception& e) {
        std::cerr << "Error evaluating predicate: " << e.what() << std::endl;
        return false;
    }
}

Value FilterExecutor::evaluateExpression(Expression* expr, const Row& row, const std::vector<ColumnInfo>& schema) {
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
            int columnIndex = findColumnIndex(identifier->name, schema);
            if (columnIndex < 0) {
                throw std::runtime_error("Column '" + identifier->name + "' not found");
            }
            return row.getValue(columnIndex);
        }
        
        case ASTNodeType::BINARY_EXPR: {
            auto* binary = static_cast<BinaryExpression*>(expr);
            Value left = evaluateExpression(binary->left.get(), row, schema);
            Value right = evaluateExpression(binary->right.get(), row, schema);
            
            // 比较操作
            if (binary->operator_ == TokenType::EQUAL ||
                binary->operator_ == TokenType::NOT_EQUAL ||
                binary->operator_ == TokenType::LESS_THAN ||
                binary->operator_ == TokenType::LESS_EQUAL ||
                binary->operator_ == TokenType::GREATER_THAN ||
                binary->operator_ == TokenType::GREATER_EQUAL) {
                
                bool compareResult = compareValues(left, right, binary->operator_);
                return compareResult ? 1 : 0;
            }
            
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
            
            // 逻辑操作
            if (binary->operator_ == TokenType::AND) {
                bool leftBool = false, rightBool = false;
                
                if (std::holds_alternative<int>(left)) leftBool = std::get<int>(left) != 0;
                if (std::holds_alternative<int>(right)) rightBool = std::get<int>(right) != 0;
                
                return (leftBool && rightBool) ? 1 : 0;
            }
            
            if (binary->operator_ == TokenType::OR) {
                bool leftBool = false, rightBool = false;
                
                if (std::holds_alternative<int>(left)) leftBool = std::get<int>(left) != 0;
                if (std::holds_alternative<int>(right)) rightBool = std::get<int>(right) != 0;
                
                return (leftBool || rightBool) ? 1 : 0;
            }
            
            throw std::runtime_error("Unsupported binary operation");
        }
        
        case ASTNodeType::UNARY_EXPR: {
            auto* unary = static_cast<UnaryExpression*>(expr);
            Value operand = evaluateExpression(unary->operand.get(), row, schema);
            
            if (unary->operator_ == TokenType::NOT) {
                if (std::holds_alternative<int>(operand)) {
                    return std::get<int>(operand) == 0 ? 1 : 0;
                }
            }
            
            throw std::runtime_error("Unsupported unary operation");
        }
        
        default:
            throw std::runtime_error("Unsupported expression type");
    }
}

bool FilterExecutor::compareValues(const Value& left, const Value& right, TokenType op) {
    // 处理相同类型的比较
    if (left.index() == right.index()) {
        if (std::holds_alternative<int>(left)) {
            int l = std::get<int>(left);
            int r = std::get<int>(right);
            switch (op) {
                case TokenType::EQUAL: return l == r;
                case TokenType::NOT_EQUAL: return l != r;
                case TokenType::LESS_THAN: return l < r;
                case TokenType::LESS_EQUAL: return l <= r;
                case TokenType::GREATER_THAN: return l > r;
                case TokenType::GREATER_EQUAL: return l >= r;
                default: break;
            }
        }
        
        if (std::holds_alternative<double>(left)) {
            double l = std::get<double>(left);
            double r = std::get<double>(right);
            switch (op) {
                case TokenType::EQUAL: return l == r;
                case TokenType::NOT_EQUAL: return l != r;
                case TokenType::LESS_THAN: return l < r;
                case TokenType::LESS_EQUAL: return l <= r;
                case TokenType::GREATER_THAN: return l > r;
                case TokenType::GREATER_EQUAL: return l >= r;
                default: break;
            }
        }
        
        if (std::holds_alternative<std::string>(left)) {
            const std::string& l = std::get<std::string>(left);
            const std::string& r = std::get<std::string>(right);
            switch (op) {
                case TokenType::EQUAL: return l == r;
                case TokenType::NOT_EQUAL: return l != r;
                case TokenType::LESS_THAN: return l < r;
                case TokenType::LESS_EQUAL: return l <= r;
                case TokenType::GREATER_THAN: return l > r;
                case TokenType::GREATER_EQUAL: return l >= r;
                default: break;
            }
        }
    }
    
    // 处理数值类型之间的比较
    if ((std::holds_alternative<int>(left) && std::holds_alternative<double>(right)) ||
        (std::holds_alternative<double>(left) && std::holds_alternative<int>(right))) {
        
        double l = std::holds_alternative<int>(left) ? 
                   static_cast<double>(std::get<int>(left)) : std::get<double>(left);
        double r = std::holds_alternative<int>(right) ? 
                   static_cast<double>(std::get<int>(right)) : std::get<double>(right);
        
        switch (op) {
            case TokenType::EQUAL: return l == r;
            case TokenType::NOT_EQUAL: return l != r;
            case TokenType::LESS_THAN: return l < r;
            case TokenType::LESS_EQUAL: return l <= r;
            case TokenType::GREATER_THAN: return l > r;
            case TokenType::GREATER_EQUAL: return l >= r;
            default: break;
        }
    }
    
    return false;
}

int FilterExecutor::findColumnIndex(const std::string& columnName, const std::vector<ColumnInfo>& schema) {
    for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].name == columnName) {
            return static_cast<int>(i);
        }
    }
    return -1;
}
