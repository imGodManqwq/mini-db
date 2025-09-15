#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// CREATE TABLE执行算子
class CreateTableExecutor : public Executor {
public:
    explicit CreateTableExecutor(ExecutionContext* context, CreateTableStatement* stmt)
        : Executor(context), createStmt_(stmt), executed_(false) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // CREATE TABLE没有子节点
    }
    
    std::string getType() const override { return "CreateTableExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        return {}; // CREATE TABLE没有输出模式
    }
    
private:
    CreateTableStatement* createStmt_;
    bool executed_;
};
