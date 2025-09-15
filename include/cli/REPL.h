#pragma once
#include "../storage/StorageEngine.h"
#include "../parser/Catalog.h"
#include "../parser/SemanticAnalyzer.h"
#include "../executor/ExecutionEngine.h"
#include <string>
#include <memory>
#include <vector>

class REPL {
public:
    explicit REPL(const std::string& dbPath = "./data");
    ~REPL();
    
    void run();             // 启动 REPL 主循环
    
private:
    bool running_;           // 标志是否继续运行
    std::string dbPath_;     // 数据库路径
    
    // 核心组件
    std::unique_ptr<StorageEngine> storageEngine_;
    std::shared_ptr<Catalog> catalog_;
    std::shared_ptr<SemanticAnalyzer> semanticAnalyzer_;
    std::unique_ptr<ExecutionEngine> executionEngine_;
    
    // 命令历史
    std::vector<std::string> commandHistory_;
    size_t maxHistorySize_;
    
    // 初始化和清理
    bool initialize();
    void cleanup();
    
    // 输入处理
    void handleInput(const std::string& input);
    void handleSQL(const std::string& sql);
    void handleMetaCommand(const std::string& command);
    std::string readCompleteStatement(const std::string& firstLine);
    
    // 元命令处理
    void showHelp();
    void showTables();
    void showSchema(const std::string& tableName = "");
    void showHistory();
    void clearScreen();
    void showStats();
    void saveDatabase();
    void showVersion();
    
    // 结果显示
    void displayQueryResults(const ExecutionResult& result);
    void displayError(const std::string& error);
    void displaySuccess(const std::string& message, size_t affectedRows = 0);
    
    // 工具函数
    std::string trim(const std::string& str);
    bool isMetaCommand(const std::string& input);
    std::string formatTable(const std::vector<std::vector<std::string>>& data, const std::vector<std::string>& headers);
    std::string valueToString(const Value& value);
};