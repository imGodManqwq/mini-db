#pragma once

#include "Executor.h"
#include "../parser/AST.h"
#include "../storage/Row.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <string>

class GroupByExecutor : public Executor {
public:
    GroupByExecutor(ExecutionContext* context, 
                   std::unique_ptr<Executor> child,
                   const std::vector<std::unique_ptr<Expression>>& groupByList,
                   const std::vector<std::unique_ptr<Expression>>& selectList);
    
    ~GroupByExecutor() override = default;
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_;
    }
    
    std::string getType() const override { return "GroupByExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override;

private:
    std::unique_ptr<Executor> child_;
    std::vector<Expression*> groupByColumns_;
    std::vector<Expression*> selectExpressions_;
    
    // 分组结果存储
    std::vector<Row> groupedResults_;
    size_t currentIndex_;
    bool processed_;
    
    // 辅助方法
    std::string computeGroupKey(const Row& row);
    Value evaluateExpression(Expression* expr, const Row& row);
    Value evaluateAggregateExpression(AggregateExpression* expr, const std::vector<Row>& groupRows);
    
    // 聚合计算
    Value calculateCount(const std::vector<Row>& rows, Expression* expr);
    Value calculateSum(const std::vector<Row>& rows, Expression* expr);
    Value calculateAvg(const std::vector<Row>& rows, Expression* expr);
    Value calculateMax(const std::vector<Row>& rows, Expression* expr);
    Value calculateMin(const std::vector<Row>& rows, Expression* expr);
};
