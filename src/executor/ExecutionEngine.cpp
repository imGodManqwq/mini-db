#include "../../include/executor/ExecutionEngine.h"
#include "../../include/executor/IndexScanExecutor.h"
#include "../../include/parser/AST.h"
#include <iostream>
#include <chrono>
#include <iomanip>
#include <limits>

ExecutionEngine::ExecutionEngine(StorageEngine* storage) 
    : storageEngine_(storage) {
    context_ = std::make_unique<ExecutionContext>(storage);
}

ExecutionResult ExecutionEngine::executeStatement(Statement* statement) {
    if (!statement) {
        return ExecutionResult(ExecutionResultType::ERROR, "Statement is null");
    }
    
    auto startTime = std::chrono::high_resolution_clock::now();
    stats_.totalStatements++;
    
    try {
        // 可选的语义检查（CREATE TABLE语句跳过语义检查，因为表还不存在）
        if (semanticAnalyzer_ && statement->nodeType != ASTNodeType::CREATE_TABLE_STMT && 
            !performSemanticCheck(statement)) {
            stats_.failedStatements++;
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Semantic analysis failed: " + context_->getError());
        }
        
        context_->clearError();
        context_->clearOutputRows();
        
        // 生成执行计划
        auto plan = generateExecutionPlan(statement);
        if (!plan || !plan->executor) {
            stats_.failedStatements++;
            return ExecutionResult(ExecutionResultType::ERROR, 
                                 "Failed to generate execution plan");
        }
        
        // 执行计划
        auto result = plan->executor->execute();
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        stats_.totalExecutionTime += duration;
        
        if (result.isSuccess()) {
            stats_.successfulStatements++;
            
            // 如果是CREATE TABLE语句成功执行，同步Catalog
            if (statement->nodeType == ASTNodeType::CREATE_TABLE_STMT && semanticAnalyzer_) {
                // 获取语义分析器的Catalog并同步
                auto catalog = semanticAnalyzer_->getCatalog();
                if (catalog) {
                    catalog->syncFromStorage();
                }
            }
        } else {
            stats_.failedStatements++;
        }
        
        return result;
        
    } catch (const std::exception& e) {
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        stats_.totalExecutionTime += duration;
        stats_.failedStatements++;
        
        return ExecutionResult(ExecutionResultType::ERROR, 
                             "Exception during execution: " + std::string(e.what()));
    }
}

std::vector<ExecutionResult> ExecutionEngine::executeStatements(const std::vector<std::unique_ptr<Statement>>& statements) {
    std::vector<ExecutionResult> results;
    results.reserve(statements.size());
    
    for (const auto& stmt : statements) {
        auto result = executeStatement(stmt.get());
        results.push_back(result);
        
        // 如果遇到错误，可以选择继续或停止
        if (result.isError()) {
            std::cerr << "Error executing statement: " << result.message << std::endl;
            // 继续执行其他语句
        }
    }
    
    return results;
}

std::unique_ptr<ExecutionPlan> ExecutionEngine::generateExecutionPlan(Statement* statement) {
    if (!statement) {
        return nullptr;
    }
    
    std::unique_ptr<Executor> executor;
    std::string planDesc;
    
    switch (statement->nodeType) {
        case ASTNodeType::CREATE_TABLE_STMT: {
            auto* createStmt = static_cast<CreateTableStatement*>(statement);
            executor = createCreateTableExecutor(createStmt);
            planDesc = "CreateTable(" + createStmt->tableName + ")";
            break;
        }
        
        case ASTNodeType::CREATE_INDEX_STMT: {
            auto* createIndexStmt = static_cast<CreateIndexStatement*>(statement);
            executor = createCreateIndexExecutor(createIndexStmt);
            planDesc = "CreateIndex(" + createIndexStmt->indexName + " on " + createIndexStmt->tableName + "." + createIndexStmt->columnName + ")";
            break;
        }
        
        case ASTNodeType::INSERT_STMT: {
            auto* insertStmt = static_cast<InsertStatement*>(statement);
            executor = createInsertExecutor(insertStmt);
            planDesc = "Insert(" + insertStmt->tableName + ")";
            break;
        }
        
        case ASTNodeType::SELECT_STMT: {
            auto* selectStmt = static_cast<SelectStatement*>(statement);
            executor = createSelectExecutor(selectStmt);
            planDesc = "Select(" + selectStmt->fromTable + ")";
            break;
        }
        
        case ASTNodeType::DELETE_STMT: {
            auto* deleteStmt = static_cast<DeleteStatement*>(statement);
            executor = createDeleteExecutor(deleteStmt);
            planDesc = "Delete(" + deleteStmt->tableName + ")";
            break;
        }
        
        case ASTNodeType::UPDATE_STMT: {
            auto* updateStmt = static_cast<UpdateStatement*>(statement);
            executor = createUpdateExecutor(updateStmt);
            planDesc = "Update(" + updateStmt->tableName + ")";
            break;
        }
        
        default:
            return nullptr;
    }
    
    if (!executor) {
        return nullptr;
    }
    
    // 应用查询优化（如果启用）
    if (optimizationEnabled_ && queryOptimizer_ && 
        (statement->nodeType == ASTNodeType::SELECT_STMT || 
         statement->nodeType == ASTNodeType::DELETE_STMT)) {
        executor = queryOptimizer_->optimize(std::move(executor));
    }
    
    // 生成详细的计划描述
    std::string detailedDesc = generatePlanDescription(executor.get());
    
    return std::make_unique<ExecutionPlan>(std::move(executor), detailedDesc);
}

void ExecutionEngine::printExecutionPlan(const ExecutionPlan& plan) const {
    std::cout << "=== Execution Plan ===" << std::endl;
    std::cout << plan.planDescription << std::endl;
    std::cout << "======================" << std::endl;
}

void ExecutionEngine::printStats() const {
    std::cout << "=== Execution Engine Statistics ===" << std::endl;
    std::cout << "Total statements executed: " << stats_.totalStatements << std::endl;
    std::cout << "Successful statements: " << stats_.successfulStatements << std::endl;
    std::cout << "Failed statements: " << stats_.failedStatements << std::endl;
    std::cout << "Total execution time: " << stats_.totalExecutionTime.count() << " ms" << std::endl;
    
    if (stats_.totalStatements > 0) {
        double successRate = (double)stats_.successfulStatements / stats_.totalStatements * 100.0;
        std::cout << "Success rate: " << std::fixed << std::setprecision(2) << successRate << "%" << std::endl;
        
        double avgTime = (double)stats_.totalExecutionTime.count() / stats_.totalStatements;
        std::cout << "Average execution time: " << std::fixed << std::setprecision(2) << avgTime << " ms" << std::endl;
    }
    std::cout << "===================================" << std::endl;
}

std::unique_ptr<Executor> ExecutionEngine::createCreateTableExecutor(CreateTableStatement* stmt) {
    return std::make_unique<CreateTableExecutor>(context_.get(), stmt);
}

std::unique_ptr<Executor> ExecutionEngine::createCreateIndexExecutor(CreateIndexStatement* stmt) {
    return std::make_unique<CreateIndexExecutor>(context_.get(), stmt);
}

std::unique_ptr<Executor> ExecutionEngine::createInsertExecutor(InsertStatement* stmt) {
    return std::make_unique<InsertExecutor>(context_.get(), stmt);
}

std::unique_ptr<Executor> ExecutionEngine::createSelectExecutor(SelectStatement* stmt) {
    // 构建查询执行计划：IndexScan/SeqScan -> Join -> Filter -> GroupBy -> Project -> OrderBy
    
    // 1. 创建扫描算子（优化：选择IndexScan或SeqScan）
    std::unique_ptr<Executor> leftScan = createOptimalScanExecutor(stmt->fromTable, stmt->whereClause.get());
    
    std::unique_ptr<Executor> current = std::move(leftScan);
    
    // 2. 处理JOIN子句
    for (const auto& joinClause : stmt->joinClauses) {
        // 为右表创建SeqScan
        auto rightScan = std::make_unique<SeqScanExecutor>(context_.get(), joinClause->rightTable);
        
        // 创建JOIN执行器
        current = std::make_unique<NestedLoopJoinExecutor>(
            std::move(current),
            std::move(rightScan),
            joinClause->joinType,
            joinClause->onCondition.get(), // 直接使用原始指针，避免双重删除
            context_.get()
        );
    }
    
    // 2. 如果有WHERE子句，添加Filter算子
    if (stmt->whereClause) {
        current = std::make_unique<FilterExecutor>(context_.get(), std::move(current), stmt->whereClause.get());
    }
    
    // 3. 检查是否有聚合函数或GROUP BY
    bool hasAggregates = false;
    for (const auto& expr : stmt->selectList) {
        if (expr->nodeType == ASTNodeType::AGGREGATE_EXPR) {
            hasAggregates = true;
            break;
        }
    }
    
    if (!stmt->groupByList.empty() || hasAggregates) {
        // 如果有GROUP BY或聚合函数，使用GroupByExecutor处理
        current = std::make_unique<GroupByExecutor>(context_.get(), std::move(current), stmt->groupByList, stmt->selectList);
    } else {
        // 否则使用常规的Project算子
        std::vector<Expression*> projections;
        for (const auto& expr : stmt->selectList) {
            projections.push_back(expr.get());
        }
        current = std::make_unique<ProjectExecutor>(context_.get(), std::move(current), projections);
    }
    
    // 5. 如果有ORDER BY子句，添加OrderBy算子
    if (!stmt->orderByList.empty()) {
        current = std::make_unique<OrderByExecutor>(context_.get(), std::move(current), stmt->orderByList);
    }
    
    return current;
}

std::unique_ptr<Executor> ExecutionEngine::createDeleteExecutor(DeleteStatement* stmt) {
    // 简化实现：DELETE暂时不实现完整的执行器
    // 在实际项目中，这里应该创建一个DeleteExecutor
    context_->setError("DELETE executor not yet implemented");
    return nullptr;
}

std::unique_ptr<Executor> ExecutionEngine::createUpdateExecutor(UpdateStatement* stmt) {
    return std::make_unique<UpdateExecutor>(context_.get(), stmt);
}

std::unique_ptr<Executor> ExecutionEngine::createOptimalScanExecutor(const std::string& tableName, Expression* whereClause) {
    // 基础索引选择逻辑：简单等值查询优化，复杂优化交给QueryOptimizer
    
    if (!whereClause) {
        // 没有WHERE子句，使用顺序扫描
        std::cout << "No WHERE clause, using sequential scan" << std::endl;
        return std::make_unique<SeqScanExecutor>(context_.get(), tableName);
    }
    
    // 检查WHERE子句是否是简单的等值或范围查询
    if (auto* binaryExpr = dynamic_cast<BinaryExpression*>(whereClause)) {
        if (auto* leftIdent = dynamic_cast<IdentifierExpression*>(binaryExpr->left.get())) {
            std::string columnName = leftIdent->name;
            
            // 检查该列是否有索引
            auto storage = context_->getStorageEngine();
        if (storage) {
            std::string indexName = findIndexForColumn(tableName, columnName);
            if (!indexName.empty()) {
                // 根据操作符类型选择索引扫描策略
                switch (binaryExpr->operator_) {
                    case TokenType::EQUAL: {
                        // 等值查询，使用精确索引扫描
                        if (auto* literal = dynamic_cast<LiteralExpression*>(binaryExpr->right.get())) {
                            std::cout << "Using index " << indexName << " for equality search on " << columnName << std::endl;
                            auto indexScan = std::make_unique<IndexScanExecutor>(context_.get(), tableName, indexName, literal->value);
                            // 立即检查索引扫描是否可以初始化
                            if (indexScan->init()) {
                                return std::move(indexScan);
                            } else {
                                std::cout << "Index scan initialization failed, falling back to sequential scan" << std::endl;
                            }
                        }
                        break;
                    }
                    case TokenType::GREATER_THAN:
                    case TokenType::GREATER_EQUAL:
                    case TokenType::LESS_THAN:
                    case TokenType::LESS_EQUAL: {
                        // 范围查询，使用索引范围扫描
                        if (auto* literal = dynamic_cast<LiteralExpression*>(binaryExpr->right.get())) {
                            std::cout << "Using index " << indexName << " for range query on " << columnName << std::endl;
                            
                            // 根据操作符类型确定范围查询的边界
                            Value startKey, endKey;
                            bool useStartKey = false, useEndKey = false;
                            
                            // 对于范围查询，我们需要特殊处理，因为B+树的findRecordsInRange使用闭区间[start,end]
                            // 但SQL的 > 和 < 是开区间，>= 和 <= 是闭区间
                            switch (binaryExpr->operator_) {
                                case TokenType::GREATER_THAN: {
                                    // salary > 70000：需要找到 > 70000 的记录
                                    // 由于findRecordsInRange使用闭区间，我们稍微增加startKey的值
                                    if (std::holds_alternative<double>(literal->value)) {
                                        double val = std::get<double>(literal->value);
                                        startKey = Value(val + 0.01);  // 稍微增加以排除等于的情况
                                    } else if (std::holds_alternative<int>(literal->value)) {
                                        int val = std::get<int>(literal->value);
                                        startKey = Value(static_cast<double>(val) + 0.01);  // 转换为double并稍微增加
                                    } else {
                                        startKey = literal->value;
                                    }
                                    useStartKey = true;
                                    endKey = Value(1000000.0);  // 上界
                                    useEndKey = true;
                                    break;
                                }
                                case TokenType::GREATER_EQUAL:
                                    // salary >= 70000：直接使用原值，但确保类型一致
                                    if (std::holds_alternative<int>(literal->value)) {
                                        int val = std::get<int>(literal->value);
                                        startKey = Value(static_cast<double>(val));  // 转换为double
                                    } else {
                                        startKey = literal->value;
                                    }
                                    useStartKey = true;
                                    endKey = Value(1000000.0);  // 上界
                                    useEndKey = true;
                                    break;
                                case TokenType::LESS_THAN: {
                                    // salary < 70000：需要找到 < 70000 的记录
                                    startKey = Value(0.0);  // 下界
                                    useStartKey = true;
                                    if (std::holds_alternative<double>(literal->value)) {
                                        double val = std::get<double>(literal->value);
                                        endKey = Value(val - 0.01);  // 稍微减少以排除等于的情况
                                    } else if (std::holds_alternative<int>(literal->value)) {
                                        int val = std::get<int>(literal->value);
                                        endKey = Value(static_cast<double>(val) - 0.01);  // 转换为double并稍微减少
                                    } else {
                                        endKey = literal->value;
                                    }
                                    useEndKey = true;
                                    break;
                                }
                                case TokenType::LESS_EQUAL:
                                    // salary <= 70000：直接使用原值，但确保类型一致
                                    startKey = Value(0.0);  // 下界
                                    useStartKey = true;
                                    if (std::holds_alternative<int>(literal->value)) {
                                        int val = std::get<int>(literal->value);
                                        endKey = Value(static_cast<double>(val));  // 转换为double
                                    } else {
                                        endKey = literal->value;
                                    }
                                    useEndKey = true;
                                    break;
                            }
                            
                            if (useStartKey && useEndKey) {
                                auto indexScan = std::make_unique<IndexScanExecutor>(context_.get(), tableName, indexName, startKey, endKey);
                                // 立即检查索引扫描是否可以初始化
                                if (indexScan->init()) {
                                    return std::move(indexScan);
                                } else {
                                    std::cout << "Range index scan initialization failed, falling back to sequential scan" << std::endl;
                                }
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        }
    }
    
    // 默认使用顺序扫描
    std::cout << "No suitable index found, using sequential scan" << std::endl;
    return std::make_unique<SeqScanExecutor>(context_.get(), tableName);
}

std::string ExecutionEngine::findIndexForColumn(const std::string& tableName, const std::string& columnName) {
    auto storage = context_->getStorageEngine();
    if (!storage) {
        return "";
    }
    
    // 检查常见的索引命名模式
    std::vector<std::string> possibleIndexNames = {
        "idx_" + columnName,                    // idx_age
        "idx_" + tableName + "_" + columnName,  // idx_employees_age  
        "pk_" + tableName + "_" + columnName    // pk_employees_id (主键索引)
    };
    
    for (const auto& indexName : possibleIndexNames) {
        // 使用新的 indexExists 方法检查索引是否存在
        if (storage->indexExists(indexName)) {
            return indexName;
        }
    }
    
    std::cout << "No suitable index found, using sequential scan" << std::endl;
    return "";
}

std::string ExecutionEngine::generatePlanDescription(Executor* executor, int depth) const {
    if (!executor) {
        return "";
    }
    
    std::string indent(depth * 2, ' ');
    std::string desc = indent + executor->getType();
    
    // 添加算子特定信息
    if (auto* seqScan = dynamic_cast<SeqScanExecutor*>(executor)) {
        desc += "(" + seqScan->getTableName() + ")";
    }
    
    desc += "\n";
    
    // 递归处理子节点
    for (const auto& child : executor->getChildren()) {
        desc += generatePlanDescription(child.get(), depth + 1);
    }
    
    return desc;
}

bool ExecutionEngine::performSemanticCheck(Statement* statement) {
    if (!semanticAnalyzer_) {
        return true; // 没有语义分析器，跳过检查
    }
    
    auto result = semanticAnalyzer_->analyzeStatement(statement);
    if (!result.success) {
        std::string errorMsg = "Semantic errors: ";
        for (const auto& error : result.errors) {
            errorMsg += error.toString() + "; ";
        }
        context_->setError(errorMsg);
        return false;
    }
    
    return true;
}
