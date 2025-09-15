#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// 投影执行算子
class ProjectExecutor : public Executor {
public:
    explicit ProjectExecutor(ExecutionContext* context, 
                           std::unique_ptr<Executor> child, 
                           const std::vector<Expression*>& projections)
        : Executor(context), projections_(projections) {
        children_.push_back(std::move(child));
    }
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_;
    }
    
    std::string getType() const override { return "ProjectExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override;
    
private:
    std::vector<Expression*> projections_;
    std::vector<ColumnInfo> outputSchema_;
    
    // 辅助方法
    Value evaluateExpression(Expression* expr, const Row& row, const std::vector<ColumnInfo>& inputSchema);
    int findColumnIndex(const std::string& columnName, const std::vector<ColumnInfo>& schema);
    std::string getExpressionName(Expression* expr);
    DataType inferExpressionType(Expression* expr, const std::vector<ColumnInfo>& inputSchema);
};
