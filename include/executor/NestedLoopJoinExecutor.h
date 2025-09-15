#pragma once
#include "Executor.h"
#include "../parser/AST.h"

class NestedLoopJoinExecutor : public Executor {
public:
    NestedLoopJoinExecutor(
        std::unique_ptr<Executor> leftChild,
        std::unique_ptr<Executor> rightChild,
        JoinType joinType,
        Expression* joinCondition,
        ExecutionContext* context
    );
    
    bool init() override;
    ExecutionResult next() override;
    std::vector<ColumnInfo> getOutputSchema() const override;
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override;
    std::string getType() const override;

private:
    std::unique_ptr<Executor> leftChild_;
    std::unique_ptr<Executor> rightChild_;
    JoinType joinType_;
    Expression* joinCondition_;
    std::vector<std::unique_ptr<Executor>> children_; // 为了满足接口要求
    
    // 状态管理
    bool leftExhausted_;
    bool rightExhausted_;
    Row currentLeftRow_;
    bool hasCurrentLeftRow_;
    std::vector<Row> rightRows_; // 缓存右表的所有行
    
    // 辅助方法
    bool evaluateJoinCondition(const Row& leftRow, const Row& rightRow);
    Row combineRows(const Row& leftRow, const Row& rightRow);
    std::vector<ColumnInfo> combineSchemas(const std::vector<ColumnInfo>& leftSchema, const std::vector<ColumnInfo>& rightSchema) const;
    Value evaluateExpressionWithBothRows(Expression* expr, const Row& leftRow, const Row& rightRow);
    
    // JOIN类型特定逻辑
    bool shouldIncludeInResult(bool conditionMet, bool hasRightMatch);
};
