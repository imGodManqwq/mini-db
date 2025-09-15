#include "../../include/executor/GroupByExecutor.h"
#include <algorithm>
#include <sstream>

GroupByExecutor::GroupByExecutor(ExecutionContext* context,
                                std::unique_ptr<Executor> child,
                                const std::vector<std::unique_ptr<Expression>>& groupByList,
                                const std::vector<std::unique_ptr<Expression>>& selectList)
    : Executor(context), child_(std::move(child)), currentIndex_(0), processed_(false) {
    
    // 转换智能指针为原始指针
    for (const auto& expr : groupByList) {
        groupByColumns_.push_back(expr.get());
    }
    
    for (const auto& expr : selectList) {
        selectExpressions_.push_back(expr.get());
    }
}

bool GroupByExecutor::init() {
    if (!child_->init()) {
        return false;
    }
    
    return true;
}

ExecutionResult GroupByExecutor::next() {
    if (!processed_) {
        // 第一次调用时处理所有数据
        std::unordered_map<std::string, std::vector<Row>> groups;
        
        // 收集所有子执行器的行
        while (true) {
            ExecutionResult childResult = child_->next();
            if (childResult.isEndOfData()) {
                break;
            }
            if (childResult.isError()) {
                return childResult;
            }
            
            // 处理子执行器返回的所有行
            for (const auto& row : childResult.rows) {
                std::string groupKey;
                if (groupByColumns_.empty()) {
                    // 没有GROUP BY时，所有行属于同一组
                    groupKey = "all_rows";
                } else {
                    groupKey = computeGroupKey(row);
                }
                groups[groupKey].push_back(row);
            }
        }
        
        // 对每个分组处理聚合结果
        for (const auto& group : groups) {
            const std::vector<Row>& groupRows = group.second;
            if (!groupRows.empty()) {
                // 基于selectList创建结果行
                std::vector<Value> resultValues;
                
                for (Expression* selectExpr : selectExpressions_) {
                    if (selectExpr->nodeType == ASTNodeType::AGGREGATE_EXPR) {
                        // 处理聚合函数
                        auto* aggExpr = static_cast<AggregateExpression*>(selectExpr);
                        Value aggResult = evaluateAggregateExpression(aggExpr, groupRows);
                        resultValues.push_back(aggResult);
                    } else {
                        // 处理非聚合表达式（通常是GROUP BY列）
                        Value value = evaluateExpression(selectExpr, groupRows[0]);
                        resultValues.push_back(value);
                    }
                }
                
                Row resultRow(resultValues);
                groupedResults_.push_back(resultRow);
            }
        }
        
        processed_ = true;
    }
    
    // 返回一行结果
    if (currentIndex_ < groupedResults_.size()) {
        ExecutionResult result;
        result.type = ExecutionResultType::SUCCESS;
        result.rows.push_back(groupedResults_[currentIndex_++]);
        return result;
    } else {
        // 没有更多行了
        return ExecutionResult(ExecutionResultType::END_OF_DATA, "No more rows");
    }
}

std::vector<ColumnInfo> GroupByExecutor::getOutputSchema() const {
    std::vector<ColumnInfo> schema;
    
    for (Expression* selectExpr : selectExpressions_) {
        if (selectExpr->nodeType == ASTNodeType::AGGREGATE_EXPR) {
            auto* aggExpr = static_cast<AggregateExpression*>(selectExpr);
            
            // 根据聚合函数类型确定输出类型
            DataType outputType = DataType::INT;
            std::string columnName = "agg_result";
            
            switch (aggExpr->function) {
                case TokenType::COUNT:
                    outputType = DataType::INT;
                    columnName = "COUNT";
                    break;
                case TokenType::SUM:
                    outputType = DataType::DOUBLE; // 简化为always double
                    columnName = "SUM";
                    break;
                case TokenType::AVG:
                    outputType = DataType::DOUBLE;
                    columnName = "AVG";
                    break;
                case TokenType::MAX:
                case TokenType::MIN:
                    outputType = DataType::DOUBLE; // 简化处理
                    columnName = (aggExpr->function == TokenType::MAX) ? "MAX" : "MIN";
                    break;
                default:
                    break;
            }
            
            schema.emplace_back(columnName, outputType);
        } else if (selectExpr->nodeType == ASTNodeType::IDENTIFIER_EXPR) {
            auto* identExpr = static_cast<IdentifierExpression*>(selectExpr);
            
            // 从child schema中查找类型
            auto childSchema = child_->getOutputSchema();
            for (const auto& col : childSchema) {
                if (col.name == identExpr->name) {
                    schema.push_back(col);
                    break;
                }
            }
        } else {
            // 其他表达式，默认为字符串类型
            schema.emplace_back("expr_result", DataType::STRING);
        }
    }
    
    return schema;
}

std::string GroupByExecutor::computeGroupKey(const Row& row) {
    std::ostringstream oss;
    
    for (size_t i = 0; i < groupByColumns_.size(); ++i) {
        if (i > 0) {
            oss << "|";
        }
        
        Value value = evaluateExpression(groupByColumns_[i], row);
        
        if (std::holds_alternative<int>(value)) {
            oss << std::get<int>(value);
        } else if (std::holds_alternative<double>(value)) {
            oss << std::get<double>(value);
        } else if (std::holds_alternative<std::string>(value)) {
            oss << std::get<std::string>(value);
        }
    }
    
    return oss.str();
}

Value GroupByExecutor::evaluateExpression(Expression* expr, const Row& row) {
    if (!expr) {
        return Value(0);
    }
    
    // 获取child的schema来解析列名
    std::vector<ColumnInfo> schema = child_->getOutputSchema();
    
    switch (expr->nodeType) {
        case ASTNodeType::IDENTIFIER_EXPR: {
            auto* identifier = static_cast<IdentifierExpression*>(expr);
            
            // 查找列索引
            for (size_t i = 0; i < schema.size(); ++i) {
                if (schema[i].name == identifier->name) {
                    if (i < row.getFieldCount()) {
                        return row.getValue(i);
                    }
                    break;
                }
            }
            return Value(0);
        }
        
        case ASTNodeType::LITERAL_EXPR: {
            auto* literal = static_cast<LiteralExpression*>(expr);
            return literal->value;
        }
        
        default:
            return Value(0);
    }
}

Value GroupByExecutor::evaluateAggregateExpression(AggregateExpression* expr, const std::vector<Row>& groupRows) {
    switch (expr->function) {
        case TokenType::COUNT:
            return calculateCount(groupRows, expr->argument.get());
        case TokenType::SUM:
            return calculateSum(groupRows, expr->argument.get());
        case TokenType::AVG:
            return calculateAvg(groupRows, expr->argument.get());
        case TokenType::MAX:
            return calculateMax(groupRows, expr->argument.get());
        case TokenType::MIN:
            return calculateMin(groupRows, expr->argument.get());
        default:
            return Value(0);
    }
}

Value GroupByExecutor::calculateCount(const std::vector<Row>& rows, Expression* expr) {
    // COUNT(*)或COUNT(column)都返回行数
    return Value(static_cast<int>(rows.size()));
}

Value GroupByExecutor::calculateSum(const std::vector<Row>& rows, Expression* expr) {
    double sum = 0.0;
    int intSum = 0;
    bool isDouble = false;
    
    for (const auto& row : rows) {
        Value value = evaluateExpression(expr, row);
        if (std::holds_alternative<int>(value)) {
            intSum += std::get<int>(value);
        } else if (std::holds_alternative<double>(value)) {
            sum += std::get<double>(value);
            isDouble = true;
        }
    }
    
    if (isDouble) {
        return Value(sum + intSum);
    } else {
        return Value(intSum);
    }
}

Value GroupByExecutor::calculateAvg(const std::vector<Row>& rows, Expression* expr) {
    if (rows.empty()) {
        return Value(0.0);
    }
    
    Value sumValue = calculateSum(rows, expr);
    double sum = 0.0;
    
    if (std::holds_alternative<int>(sumValue)) {
        sum = static_cast<double>(std::get<int>(sumValue));
    } else if (std::holds_alternative<double>(sumValue)) {
        sum = std::get<double>(sumValue);
    }
    
    return Value(sum / rows.size());
}

Value GroupByExecutor::calculateMax(const std::vector<Row>& rows, Expression* expr) {
    if (rows.empty()) {
        return Value(0);
    }
    
    Value maxValue = evaluateExpression(expr, rows[0]);
    
    for (size_t i = 1; i < rows.size(); ++i) {
        Value currentValue = evaluateExpression(expr, rows[i]);
        
        // 简化比较逻辑
        if (std::holds_alternative<int>(maxValue) && std::holds_alternative<int>(currentValue)) {
            if (std::get<int>(currentValue) > std::get<int>(maxValue)) {
                maxValue = currentValue;
            }
        } else if (std::holds_alternative<double>(maxValue) && std::holds_alternative<double>(currentValue)) {
            if (std::get<double>(currentValue) > std::get<double>(maxValue)) {
                maxValue = currentValue;
            }
        }
    }
    
    return maxValue;
}

Value GroupByExecutor::calculateMin(const std::vector<Row>& rows, Expression* expr) {
    if (rows.empty()) {
        return Value(0);
    }
    
    Value minValue = evaluateExpression(expr, rows[0]);
    
    for (size_t i = 1; i < rows.size(); ++i) {
        Value currentValue = evaluateExpression(expr, rows[i]);
        
        // 简化比较逻辑
        if (std::holds_alternative<int>(minValue) && std::holds_alternative<int>(currentValue)) {
            if (std::get<int>(currentValue) < std::get<int>(minValue)) {
                minValue = currentValue;
            }
        } else if (std::holds_alternative<double>(minValue) && std::holds_alternative<double>(currentValue)) {
            if (std::get<double>(currentValue) < std::get<double>(minValue)) {
                minValue = currentValue;
            }
        }
    }
    
    return minValue;
}