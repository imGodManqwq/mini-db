#pragma once
#include "Executor.h"
#include "CreateTableExecutor.h"
#include "CreateIndexExecutor.h"
#include "InsertExecutor.h"
#include "UpdateExecutor.h"
#include "SeqScanExecutor.h"
#include "FilterExecutor.h"
#include "ProjectExecutor.h"
#include "GroupByExecutor.h"
#include "OrderByExecutor.h"
#include "NestedLoopJoinExecutor.h"
#include "QueryOptimizer.h"
#include "../parser/AST.h"
#include "../parser/SemanticAnalyzer.h"
#include <memory>
#include <string>
#include <vector>
#include <chrono>

// 执行计划节点
struct ExecutionPlan {
    std::unique_ptr<Executor> executor;
    std::string planDescription;
    
    ExecutionPlan(std::unique_ptr<Executor> exec, const std::string& desc)
        : executor(std::move(exec)), planDescription(desc) {}
};

// 执行引擎 - 负责将AST转换为执行计划并执行
class ExecutionEngine {
public:
    explicit ExecutionEngine(StorageEngine* storage);
    ~ExecutionEngine() = default;
    
    // 执行单个语句
    ExecutionResult executeStatement(Statement* statement);
    
    // 执行多个语句
    std::vector<ExecutionResult> executeStatements(const std::vector<std::unique_ptr<Statement>>& statements);
    
    // 生成执行计划（不执行）
    std::unique_ptr<ExecutionPlan> generateExecutionPlan(Statement* statement);
    
    // 打印执行计划
    void printExecutionPlan(const ExecutionPlan& plan) const;
    
    // 设置语义分析器（可选，用于额外验证）
    void setSemanticAnalyzer(std::shared_ptr<SemanticAnalyzer> analyzer) {
        semanticAnalyzer_ = analyzer;
    }
    
    // 设置查询优化器（可选，用于查询优化）
    void setQueryOptimizer(std::unique_ptr<QueryOptimizer> optimizer) {
        queryOptimizer_ = std::move(optimizer);
    }
    
    // 启用/禁用查询优化
    void enableOptimization(bool enabled = true) { optimizationEnabled_ = enabled; }
    void disableOptimization() { optimizationEnabled_ = false; }
    
    // 统计信息
    struct ExecutionStats {
        size_t totalStatements;
        size_t successfulStatements;
        size_t failedStatements;
        std::chrono::milliseconds totalExecutionTime;
        
        ExecutionStats() : totalStatements(0), successfulStatements(0), 
                          failedStatements(0), totalExecutionTime(0) {}
    };
    
    const ExecutionStats& getStats() const { return stats_; }
    void resetStats() { stats_ = ExecutionStats(); }
    void printStats() const;
    
private:
    StorageEngine* storageEngine_;
    std::unique_ptr<ExecutionContext> context_;
    std::shared_ptr<SemanticAnalyzer> semanticAnalyzer_;
    std::unique_ptr<QueryOptimizer> queryOptimizer_;
    bool optimizationEnabled_ = true;
    ExecutionStats stats_;
    
    // 执行计划生成方法
    std::unique_ptr<Executor> createCreateTableExecutor(CreateTableStatement* stmt);
    std::unique_ptr<Executor> createDropTableExecutor(DropTableStatement* stmt);
    std::unique_ptr<Executor> createCreateIndexExecutor(CreateIndexStatement* stmt);
    std::unique_ptr<Executor> createInsertExecutor(InsertStatement* stmt);
    std::unique_ptr<Executor> createSelectExecutor(SelectStatement* stmt);
    std::unique_ptr<Executor> createDeleteExecutor(DeleteStatement* stmt);
    std::unique_ptr<Executor> createUpdateExecutor(UpdateStatement* stmt);
    
    // 查询优化：选择最优的扫描算子
    std::unique_ptr<Executor> createOptimalScanExecutor(const std::string& tableName, Expression* whereClause);
    
    // 查找列对应的索引名
    std::string findIndexForColumn(const std::string& tableName, const std::string& columnName);
    
    // 辅助方法
    std::string generatePlanDescription(Executor* executor, int depth = 0) const;
    bool performSemanticCheck(Statement* statement);
};
