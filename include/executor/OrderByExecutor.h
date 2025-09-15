#pragma once

#include "Executor.h"
#include "../parser/AST.h"
#include "../storage/Row.h"
#include <vector>
#include <memory>

class OrderByExecutor : public Executor {
public:
    OrderByExecutor(ExecutionContext* context,
                   std::unique_ptr<Executor> child,
                   const std::vector<std::unique_ptr<OrderByItem>>& orderByList);
    
    ~OrderByExecutor() override = default;
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_;
    }
    
    std::string getType() const override { return "OrderByExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override;

private:
    std::unique_ptr<Executor> child_;
    std::vector<OrderByItem*> orderByColumns_;
    
    // 排序结果存储
    std::vector<Row> sortedResults_;
    size_t currentIndex_;
    bool processed_;
    
    // 辅助方法
    Value evaluateExpression(Expression* expr, const Row& row);
    bool compareRows(const Row& a, const Row& b);
};
