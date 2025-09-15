#pragma once
#include "Executor.h"
#include "../parser/AST.h"
#include "../storage/StorageEngine.h"
#include <vector>
#include <memory>
#include <unordered_map>
#include <chrono>
#include <string>

// 前向声明
class SeqScanExecutor;
class FilterExecutor;
class IndexScanExecutor;
class ProjectExecutor;

// 优化规则接口
class OptimizationRule {
public:
    virtual ~OptimizationRule() = default;
    virtual bool canApply(const std::unique_ptr<Executor>& executor) const = 0;
    virtual std::unique_ptr<Executor> apply(std::unique_ptr<Executor> executor) const = 0;
    virtual std::string getRuleName() const = 0;
    virtual int getPriority() const { return 0; } // 优先级，数字越大优先级越高
};

// 索引选择规则 - 将顺序扫描+过滤器转换为索引扫描
class IndexSelectionRule : public OptimizationRule {
public:
    IndexSelectionRule(StorageEngine* storage) : storage_(storage) {}
    
    bool canApply(const std::unique_ptr<Executor>& executor) const override;
    std::unique_ptr<Executor> apply(std::unique_ptr<Executor> executor) const override;
    std::string getRuleName() const override { return "IndexSelection"; }
    int getPriority() const override { return 10; } // 高优先级

private:
    StorageEngine* storage_;
    
    // 分析WHERE条件，寻找可用索引
    std::string findBestIndex(const std::string& tableName, Expression* condition) const;
    
    // 评估索引选择性（简化版本）
    double estimateSelectivity(const std::string& tableName, 
                              const std::string& columnName,
                              Expression* condition) const;
};

// 谓词下推规则 - 将过滤条件尽可能推到数据源附近
class PredicatePushdownRule : public OptimizationRule {
public:
    bool canApply(const std::unique_ptr<Executor>& executor) const override;
    std::unique_ptr<Executor> apply(std::unique_ptr<Executor> executor) const override;
    std::string getRuleName() const override { return "PredicatePushdown"; }
    int getPriority() const override { return 5; }

private:
    // 检查是否可以将过滤条件推下
    bool canPushDown(Expression* condition, const std::vector<ColumnInfo>& availableColumns) const;
    
    // 检查条件是否可以下推到指定表的扫描
    bool canPushDownToScan(Expression* condition, const std::string& tableName) const;
    
    // 创建带过滤条件的扫描执行器
    std::unique_ptr<Executor> createFilteredScanExecutor(SeqScanExecutor* originalScan, Expression* condition) const;
};

// 冗余操作消除规则
class RedundantOperationEliminationRule : public OptimizationRule {
public:
    bool canApply(const std::unique_ptr<Executor>& executor) const override;
    std::unique_ptr<Executor> apply(std::unique_ptr<Executor> executor) const override;
    std::string getRuleName() const override { return "RedundantOperationElimination"; }
    int getPriority() const override { return 3; }

private:
    // 检查是否有冗余的投影操作
    bool hasRedundantProjection(const std::unique_ptr<Executor>& executor) const;
};

// 查询优化器
class QueryOptimizer {
public:
    explicit QueryOptimizer(StorageEngine* storage);
    
    // 优化执行计划
    std::unique_ptr<Executor> optimize(std::unique_ptr<Executor> executor);
    
    // 添加优化规则
    void addRule(std::unique_ptr<OptimizationRule> rule);
    
    // 启用/禁用特定规则
    void enableRule(const std::string& ruleName, bool enabled = true);
    void disableRule(const std::string& ruleName) { enableRule(ruleName, false); }
    
    // 获取优化统计信息
    struct OptimizationStats {
        size_t totalOptimizations = 0;
        size_t rulesApplied = 0;
        std::unordered_map<std::string, size_t> ruleApplicationCount;
        std::chrono::milliseconds optimizationTime{0};
        
        OptimizationStats() = default;
    };
    
    const OptimizationStats& getStats() const { return stats_; }
    void resetStats();
    void printStats() const;

private:
    StorageEngine* storage_;
    std::vector<std::unique_ptr<OptimizationRule>> rules_;
    std::unordered_map<std::string, bool> ruleEnabled_;
    OptimizationStats stats_;
    
    // 应用所有可用规则
    std::unique_ptr<Executor> applyRules(std::unique_ptr<Executor> executor);
    
    // 按优先级排序规则
    void sortRulesByPriority();
    
    // 生成优化前后的执行计划描述
    std::string generatePlanDescription(const std::unique_ptr<Executor>& executor, int depth = 0) const;
};
