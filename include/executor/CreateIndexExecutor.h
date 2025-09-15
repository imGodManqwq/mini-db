#pragma once
#include "Executor.h"
#include "../parser/AST.h"

// CREATE INDEX执行算子
class CreateIndexExecutor : public Executor {
public:
    explicit CreateIndexExecutor(ExecutionContext* context, CreateIndexStatement* createIndexStmt)
        : Executor(context), createIndexStmt_(createIndexStmt), executed_(false) {}
    
    bool init() override;
    ExecutionResult next() override;
    
    const std::vector<std::unique_ptr<Executor>>& getChildren() const override {
        return children_; // CreateIndex没有子节点
    }
    
    std::string getType() const override { return "CreateIndexExecutor"; }
    
    std::vector<ColumnInfo> getOutputSchema() const override {
        return {}; // DDL语句没有输出schema
    }
    
private:
    CreateIndexStatement* createIndexStmt_;
    bool executed_;
};
