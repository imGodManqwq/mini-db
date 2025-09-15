#pragma once
#include "../storage/Row.h"
#include "../storage/StorageEngine.h"
#include "../storage/Table.h"
#include "../parser/AST.h"
#include <vector>
#include <memory>
#include <string>

// 前向声明
class ExecutionContext;

// 执行结果类型
enum class ExecutionResultType {
    SUCCESS,
    ERROR,
    END_OF_DATA
};

// 执行结果
struct ExecutionResult {
    ExecutionResultType type;
    std::string message;
    std::vector<Row> rows;
    std::vector<ColumnInfo> columnInfo;  // 添加列信息
    size_t affectedRows;
    
    ExecutionResult(ExecutionResultType t = ExecutionResultType::SUCCESS, const std::string& msg = "")
        : type(t), message(msg), affectedRows(0) {}
    
    bool isSuccess() const { return type == ExecutionResultType::SUCCESS; }
    bool isError() const { return type == ExecutionResultType::ERROR; }
    bool isEndOfData() const { return type == ExecutionResultType::END_OF_DATA; }
};

// 执行上下文 - 包含执行所需的环境信息
class ExecutionContext {
public:
    explicit ExecutionContext(StorageEngine* storage) : storageEngine_(storage) {}
    
    StorageEngine* getStorageEngine() const { return storageEngine_; }
    
    // 添加输出行
    void addOutputRow(const Row& row) { outputRows_.push_back(row); }
    const std::vector<Row>& getOutputRows() const { return outputRows_; }
    void clearOutputRows() { outputRows_.clear(); }
    
    // 错误处理
    void setError(const std::string& error) { errorMessage_ = error; }
    const std::string& getError() const { return errorMessage_; }
    bool hasError() const { return !errorMessage_.empty(); }
    void clearError() { errorMessage_.clear(); }
    
private:
    StorageEngine* storageEngine_;
    std::vector<Row> outputRows_;
    std::string errorMessage_;
};

// 执行算子基类
class Executor {
public:
    virtual ~Executor() = default;
    
    // 初始化算子
    virtual bool init() = 0;
    
    // 获取下一个元组（行）
    virtual ExecutionResult next() = 0;
    
    // 获取算子的子节点
    virtual const std::vector<std::unique_ptr<Executor>>& getChildren() const = 0;
    
    // 获取算子类型名称
    virtual std::string getType() const = 0;
    
    // 获取算子的输出模式（列信息）
    virtual std::vector<ColumnInfo> getOutputSchema() const = 0;
    
    // 执行完整的算子（用于没有输出的算子如INSERT, CREATE TABLE）
    virtual ExecutionResult execute() {
        if (!init()) {
            return ExecutionResult(ExecutionResultType::ERROR, "Failed to initialize executor");
        }
        
        ExecutionResult result;
        while (true) {
            auto nextResult = next();
            if (nextResult.isEndOfData()) {
                break;
            }
            if (nextResult.isError()) {
                return nextResult;
            }
            
            // 累加输出行
            for (const auto& row : nextResult.rows) {
                result.rows.push_back(row);
            }
            result.affectedRows += nextResult.affectedRows;
        }
        
        // 设置输出列信息
        result.columnInfo = getOutputSchema();
        
        return result;
    }
    
    // 算子统计信息
    virtual void printStats() const {}
    
protected:
    ExecutionContext* context_;
    std::vector<std::unique_ptr<Executor>> children_;
    bool initialized_ = false;
    
    explicit Executor(ExecutionContext* context) : context_(context) {}
};
