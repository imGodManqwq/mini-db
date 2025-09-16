#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// DROP TABLE执行算子
class DropTableExecutor : public Executor {
public:
    explicit DropTableExecutor(ExecutionContext* context, DropTableStatement* stmt)
        : Executor(context), dropStmt_(stmt), finished_(false) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // DROP TABLE通常没有子节点
    }
    
    std::string getType() const override { return "DropTableExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        return {}; // DROP TABLE没有输出模式
    }
    
private:
    DropTableStatement* dropStmt_;
    bool finished_;
};
