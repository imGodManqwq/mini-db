#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// INSERT执行算子
class InsertExecutor : public Executor {
public:
    explicit InsertExecutor(ExecutionContext* context, InsertStatement* stmt)
        : Executor(context), insertStmt_(stmt), currentValueIndex_(0) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // INSERT通常没有子节点（除非有子查询）
    }
    
    std::string getType() const override { return "InsertExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        return {}; // INSERT没有输出模式
    }
    
private:
    InsertStatement* insertStmt_;
    size_t currentValueIndex_;
    
    // 辅助方法
    Value evaluateExpression(Expression* expr);
    std::vector<Value> evaluateValueList(const std::vector<std::unique_ptr<Expression>>& expressions);
};
