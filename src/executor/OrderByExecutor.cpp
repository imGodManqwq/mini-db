#include "../../include/executor/OrderByExecutor.h"
#include <algorithm>

OrderByExecutor::OrderByExecutor(ExecutionContext* context,
                                std::unique_ptr<Executor> child,
                                const std::vector<std::unique_ptr<OrderByItem>>& orderByList)
    : Executor(context), child_(std::move(child)), currentIndex_(0), processed_(false) {
    
    // 转换智能指针为原始指针
    for (const auto& item : orderByList) {
        orderByColumns_.push_back(item.get());
    }
}

bool OrderByExecutor::init() {
    if (!child_->init()) {
        return false;
    }
    
    return true;
}

ExecutionResult OrderByExecutor::next() {
    if (!processed_) {
        // 第一次调用时处理所有数据
        
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
                sortedResults_.push_back(row);
            }
        }
        
        // 对行进行排序
        std::sort(sortedResults_.begin(), sortedResults_.end(), 
                  [this](const Row& a, const Row& b) {
                      return compareRows(a, b);
                  });
        
        processed_ = true;
    }
    
    // 返回一行结果
    if (currentIndex_ < sortedResults_.size()) {
        ExecutionResult result;
        result.type = ExecutionResultType::SUCCESS;
        result.rows.push_back(sortedResults_[currentIndex_++]);
        return result;
    } else {
        // 没有更多行了
        return ExecutionResult(ExecutionResultType::END_OF_DATA, "No more rows");
    }
}

std::vector<ColumnInfo> OrderByExecutor::getOutputSchema() const {
    if (child_) {
        return child_->getOutputSchema();
    }
    return {};
}

Value OrderByExecutor::evaluateExpression(Expression* expr, const Row& row) {
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
        
        case ASTNodeType::AGGREGATE_EXPR: {
            // 对于ORDER BY中的聚合函数，这里暂时简化处理
            // 实际应该在GroupBy之后才能使用聚合函数进行排序
            return Value(0);
        }
        
        default:
            return Value(0);
    }
}

bool OrderByExecutor::compareRows(const Row& a, const Row& b) {
    for (const auto& orderItem : orderByColumns_) {
        Value valueA = evaluateExpression(orderItem->expression.get(), a);
        Value valueB = evaluateExpression(orderItem->expression.get(), b);
        
        // 比较两个值
        int cmpResult = 0;
        
        if (std::holds_alternative<int>(valueA) && std::holds_alternative<int>(valueB)) {
            int intA = std::get<int>(valueA);
            int intB = std::get<int>(valueB);
            cmpResult = (intA < intB) ? -1 : (intA > intB) ? 1 : 0;
        } else if (std::holds_alternative<double>(valueA) && std::holds_alternative<double>(valueB)) {
            double doubleA = std::get<double>(valueA);
            double doubleB = std::get<double>(valueB);
            cmpResult = (doubleA < doubleB) ? -1 : (doubleA > doubleB) ? 1 : 0;
        } else if (std::holds_alternative<std::string>(valueA) && std::holds_alternative<std::string>(valueB)) {
            const std::string& strA = std::get<std::string>(valueA);
            const std::string& strB = std::get<std::string>(valueB);
            cmpResult = strA.compare(strB);
        } else {
            // 混合类型比较，暂时跳过
            continue;
        }
        
        if (cmpResult != 0) {
            // 根据ASC/DESC决定返回值
            if (orderItem->ascending) {
                return cmpResult < 0;
            } else {
                return cmpResult > 0;
            }
        }
        
        // 如果当前列的值相等，继续比较下一列
    }
    
    // 所有比较列都相等
    return false;
}