#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// 过滤执行算子
class FilterExecutor : public Executor {
public:
    explicit FilterExecutor(ExecutionContext* context, std::unique_ptr<Executor> child, Expression* predicate)
        : Executor(context), predicate_(predicate) {
        children_.push_back(std::move(child));
    }
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_;
    }
    
    std::string getType() const override { return "FilterExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        if (!children_.empty()) {
            return children_[0]->getOutputSchema();
        }
        return {};
    }
    
    // 获取过滤条件（用于优化器）
    Expression* getCondition() const { return predicate_; }
    
    // 获取执行上下文（用于优化器）
    ExecutionContext* getContext() const { return context_; }
    
private:
    Expression* predicate_;
    
    // 辅助方法
    bool evaluatePredicate(const Row& row, const std::vector<ColumnInfo>& schema);
    Value evaluateExpression(Expression* expr, const Row& row, const std::vector<ColumnInfo>& schema);
    bool compareValues(const Value& left, const Value& right, TokenType op);
    int findColumnIndex(const std::string& columnName, const std::vector<ColumnInfo>& schema);
};
