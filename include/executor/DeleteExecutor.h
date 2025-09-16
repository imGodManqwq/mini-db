#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// DELETE执行算子
class DeleteExecutor : public Executor {
public:
    explicit DeleteExecutor(ExecutionContext* context, DeleteStatement* stmt)
        : Executor(context), deleteStmt_(stmt), finished_(false) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // DELETE通常没有子节点
    }
    
    std::string getType() const override { return "DeleteExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        return {}; // DELETE没有输出模式
    }
    
private:
    DeleteStatement* deleteStmt_;
    bool finished_;
    
    // 辅助方法
    Value evaluateExpression(Expression* expr, const Row& currentRow);
    bool evaluateWhereCondition(Expression* whereExpr, const Row& row);
};
