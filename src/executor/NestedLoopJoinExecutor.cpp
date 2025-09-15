#include "../../include/executor/NestedLoopJoinExecutor.h"
#include <iostream>

NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    std::unique_ptr<Executor> leftChild,
    std::unique_ptr<Executor> rightChild,
    JoinType joinType,
    Expression* joinCondition,
    ExecutionContext* context
) : Executor(context), 
    leftChild_(std::move(leftChild)), 
    rightChild_(std::move(rightChild)),
    joinType_(joinType),
    joinCondition_(joinCondition),
    leftExhausted_(false),
    rightExhausted_(false),
    hasCurrentLeftRow_(false) {}

bool NestedLoopJoinExecutor::init() {
    if (!leftChild_->init() || !rightChild_->init()) {
        return false;
    }
    
    // 缓存右表的所有行（简单的嵌套循环JOIN实现）
    rightRows_.clear();
    while (true) {
        ExecutionResult result = rightChild_->next();
        if (result.isEndOfData()) {
            break;
        }
        if (result.isError()) {
            return false;
        }
        
        for (const auto& row : result.rows) {
            rightRows_.push_back(row);
        }
    }
    
    return true;
}

ExecutionResult NestedLoopJoinExecutor::next() {
    std::vector<Row> resultRows;
    
    while (resultRows.empty() && !leftExhausted_) {
        // 如果没有当前左行，获取下一个左行
        if (!hasCurrentLeftRow_) {
            ExecutionResult leftResult = leftChild_->next();
            if (leftResult.isEndOfData()) {
                leftExhausted_ = true;
                break;
            }
            if (leftResult.isError()) {
                return leftResult;
            }
            
            if (!leftResult.rows.empty()) {
                currentLeftRow_ = leftResult.rows[0]; // 简化：假设每次返回一行
                hasCurrentLeftRow_ = true;
            } else {
                continue;
            }
        }
        
        // 与右表的所有行进行JOIN
        bool foundMatch = false;
        for (size_t i = 0; i < rightRows_.size(); ++i) {
            bool conditionMet = evaluateJoinCondition(currentLeftRow_, rightRows_[i]);
            
            if (shouldIncludeInResult(conditionMet, true)) {
                Row joinedRow = combineRows(currentLeftRow_, rightRows_[i]);
                resultRows.push_back(joinedRow);
                foundMatch = true;
            }
        }
        
        // 处理LEFT JOIN的情况：如果没有匹配的右行
        if (!foundMatch && (joinType_ == JoinType::LEFT || joinType_ == JoinType::FULL_OUTER)) {
            // 创建一个包含NULL值的右行
            std::vector<ColumnInfo> rightSchema = rightChild_->getOutputSchema();
            std::vector<Value> nullValues(rightSchema.size(), Value()); // 默认构造为NULL
            Row nullRightRow(nullValues);
            
            Row joinedRow = combineRows(currentLeftRow_, nullRightRow);
            resultRows.push_back(joinedRow);
        }
        
        // 移动到下一个左行
        hasCurrentLeftRow_ = false;
    }
    
    if (resultRows.empty()) {
        return ExecutionResult(ExecutionResultType::END_OF_DATA);
    }
    
    ExecutionResult result(ExecutionResultType::SUCCESS);
    result.rows = resultRows;
    return result;
}

std::vector<ColumnInfo> NestedLoopJoinExecutor::getOutputSchema() const {
    return combineSchemas(leftChild_->getOutputSchema(), rightChild_->getOutputSchema());
}

const std::vector<std::unique_ptr<Executor>>& NestedLoopJoinExecutor::getChildren() const {
    return children_;
}

std::string NestedLoopJoinExecutor::getType() const {
    return "NestedLoopJoin";
}

bool NestedLoopJoinExecutor::evaluateJoinCondition(const Row& leftRow, const Row& rightRow) {
    if (!joinCondition_) {
        return true; // 如果没有条件，认为所有行都匹配
    }
    
    // 简化实现：假设JOIN条件是一个二元表达式
    try {
        Value result = evaluateExpressionWithBothRows(joinCondition_, leftRow, rightRow);
        if (std::holds_alternative<int>(result)) {
            return std::get<int>(result) != 0;
        }
        return false;
    } catch (const std::exception& e) {
        std::cerr << "Error evaluating join condition: " << e.what() << std::endl;
        return false;
    }
}

Row NestedLoopJoinExecutor::combineRows(const Row& leftRow, const Row& rightRow) {
    std::vector<Value> combinedValues;
    
    // 添加左表的所有列
    for (size_t i = 0; i < leftRow.getFieldCount(); ++i) {
        combinedValues.push_back(leftRow.getValue(i));
    }
    
    // 添加右表的所有列
    for (size_t i = 0; i < rightRow.getFieldCount(); ++i) {
        combinedValues.push_back(rightRow.getValue(i));
    }
    
    return Row(combinedValues);
}

std::vector<ColumnInfo> NestedLoopJoinExecutor::combineSchemas(const std::vector<ColumnInfo>& leftSchema, const std::vector<ColumnInfo>& rightSchema) const {
    std::vector<ColumnInfo> combinedSchema = leftSchema;
    combinedSchema.insert(combinedSchema.end(), rightSchema.begin(), rightSchema.end());
    return combinedSchema;
}

Value NestedLoopJoinExecutor::evaluateExpressionWithBothRows(Expression* expr, const Row& leftRow, const Row& rightRow) {
    // 这是一个简化的实现
    // 在实际实现中，需要更复杂的逻辑来处理跨表的列引用
    
    if (auto binaryExpr = dynamic_cast<BinaryExpression*>(expr)) {
        Value leftVal = evaluateExpressionWithBothRows(binaryExpr->left.get(), leftRow, rightRow);
        Value rightVal = evaluateExpressionWithBothRows(binaryExpr->right.get(), leftRow, rightRow);
        
        if (binaryExpr->operator_ == TokenType::EQUAL) {
            return Value(leftVal == rightVal ? 1 : 0);
        }
        // 添加其他运算符的支持...
    } else if (auto identifierExpr = dynamic_cast<IdentifierExpression*>(expr)) {
        // 简化：假设列名直接匹配
        // 实际需要处理表前缀和列索引映射
        std::string columnName = identifierExpr->name;
        
        // 尝试在左表中查找
        std::vector<ColumnInfo> leftSchema = leftChild_->getOutputSchema();
        for (size_t i = 0; i < leftSchema.size(); ++i) {
            if (leftSchema[i].name == columnName) {
                return leftRow.getValue(i);
            }
        }
        
        // 尝试在右表中查找
        std::vector<ColumnInfo> rightSchema = rightChild_->getOutputSchema();
        for (size_t i = 0; i < rightSchema.size(); ++i) {
            if (rightSchema[i].name == columnName) {
                return rightRow.getValue(i);
            }
        }
    }
    
    return Value(); // 返回NULL
}

bool NestedLoopJoinExecutor::shouldIncludeInResult(bool conditionMet, bool hasRightMatch) {
    switch (joinType_) {
        case JoinType::INNER:
            return conditionMet;
        case JoinType::LEFT:
            return conditionMet; // 只有条件满足才包含，NULL行在外部处理
        case JoinType::RIGHT:
            return conditionMet || !hasRightMatch; // RIGHT JOIN逻辑
        case JoinType::FULL_OUTER:
            return conditionMet; // 条件满足才包含，NULL行在外部处理
        default:
            return conditionMet;
    }
}
