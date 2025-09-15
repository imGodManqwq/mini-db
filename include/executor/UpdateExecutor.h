#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// UPDATE执行算子
class UpdateExecutor : public Executor {
public:
    explicit UpdateExecutor(ExecutionContext* context, UpdateStatement* stmt)
        : Executor(context), updateStmt_(stmt), finished_(false) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // UPDATE通常没有子节点
    }
    
    std::string getType() const override { return "UpdateExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        return {}; // UPDATE没有输出模式（或可以返回受影响的行数）
    }
    
private:
    UpdateStatement* updateStmt_;
    bool finished_;
    
    // 辅助方法
    Value evaluateExpression(Expression* expr, const Row& currentRow);
    bool evaluateWhereCondition(Expression* whereExpr, const Row& row);
    std::vector<Value> evaluateAssignments(const Row& currentRow);
};
