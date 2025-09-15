#include "../../include/cli/REPL.h"
#include "../../include/parser/Parser.h"
#include "../../include/parser/Lexer.h"
#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <filesystem>

REPL::REPL(const std::string& dbPath) 
    : running_(false), dbPath_(dbPath), maxHistorySize_(100) {
}

REPL::~REPL() {
    cleanup();
}

bool REPL::initialize() {
    try {
        std::cout << "Initializing MiniDB..." << std::endl;
        
        // 确保数据库目录存在
        std::filesystem::create_directories(dbPath_);
        
        // 初始化存储引擎
        storageEngine_ = std::make_unique<StorageEngine>(dbPath_);
        std::cout << "Storage engine initialized successfully" << std::endl;
        
        // 初始化Catalog
        catalog_ = std::make_shared<Catalog>(storageEngine_.get());
        std::cout << "Catalog system initialized successfully" << std::endl;
        
        // 初始化语义分析器
        semanticAnalyzer_ = std::make_shared<SemanticAnalyzer>(catalog_);
        std::cout << "Semantic analyzer initialized successfully" << std::endl;
        
        // 初始化执行引擎
        executionEngine_ = std::make_unique<ExecutionEngine>(storageEngine_.get());
        executionEngine_->setSemanticAnalyzer(semanticAnalyzer_);
        std::cout << "Execution engine initialized successfully" << std::endl;
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize MiniDB: " << e.what() << std::endl;
        return false;
    }
}

void REPL::cleanup() {
    if (storageEngine_) {
        std::cout << "Saving database..." << std::endl;
        storageEngine_->saveToStorage();
        std::cout << "Database saved successfully." << std::endl;
    }
}

void REPL::run() {
    std::cout << "==========================================" << std::endl;
    std::cout << "    Welcome to MiniDB v1.0" << std::endl;
    std::cout << "    A Simple Relational Database System" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;
    
    if (!initialize()) {
        std::cerr << "Failed to initialize database. Exiting..." << std::endl;
        return;
    }
    
    std::cout << std::endl;
    std::cout << "Database ready! Type '.help' for help or 'exit' to quit." << std::endl;
    std::cout << "Database path: " << dbPath_ << std::endl;
    std::cout << std::endl;
    
    running_ = true;
    std::string line;
    
    while (running_) {
        std::cout << "minidb> ";
        
        if (!std::getline(std::cin, line)) {
            break; // EOF
        }
        
        line = trim(line);
        
        if (line.empty()) {
            continue;
        }
        
        if (line == "exit" || line == "quit" || line == ".exit") {
            running_ = false;
            continue;
        }
        
        // 读取完整的SQL语句（直到遇到分号或元命令）
        std::string completeStatement = readCompleteStatement(line);
        
        if (completeStatement.empty()) {
            continue; // 用户取消了输入
        }
        
        // 添加到历史记录
        if (commandHistory_.size() >= maxHistorySize_) {
            commandHistory_.erase(commandHistory_.begin());
        }
        commandHistory_.push_back(completeStatement);
        
        handleInput(completeStatement);
    }
    
    std::cout << std::endl << "Goodbye!" << std::endl;
}

std::string REPL::readCompleteStatement(const std::string& firstLine) {
    // 如果是元命令，直接返回
    if (isMetaCommand(firstLine)) {
        return firstLine;
    }
    
    std::string completeStatement = firstLine;
    
    // 检查是否以分号结尾
    while (!completeStatement.empty() && completeStatement.back() != ';') {
        std::cout << "    ... ";  // 多行输入提示符
        std::string continueLine;
        
        if (!std::getline(std::cin, continueLine)) {
            // 如果用户按Ctrl+C或EOF，返回空字符串
            std::cout << std::endl;
            return "";
        }
        
        continueLine = trim(continueLine);
        
        // 如果用户输入空行并按回车，可能想要取消当前输入
        if (continueLine.empty()) {
            std::cout << "Statement cancelled (empty line). To continue multi-line input, add content." << std::endl;
            return "";
        }
        
        // 检查是否想要取消
        if (continueLine == "\\c" || continueLine == "\\cancel") {
            std::cout << "Statement cancelled." << std::endl;
            return "";
        }
        
        // 将新行追加到完整语句中
        if (!completeStatement.empty()) {
            completeStatement += " ";
        }
        completeStatement += continueLine;
    }
    
    return completeStatement;
}

void REPL::handleInput(const std::string& input) {
    try {
        if (isMetaCommand(input)) {
            handleMetaCommand(input);
        } else {
            handleSQL(input);
        }
    } catch (const std::exception& e) {
        displayError("Exception: " + std::string(e.what()));
    }
}

void REPL::handleSQL(const std::string& sql) {
    try {
        // 解析SQL
        Parser parser(sql);
        auto statement = parser.parseStatement();
        
        if (!statement) {
            displayError("Failed to parse SQL statement");
            if (parser.hasErrors()) {
                parser.printErrors();
            }
            return;
        }
        
        // 执行SQL
        auto result = executionEngine_->executeStatement(statement.get());
        
        if (result.isSuccess()) {
            if (!result.rows.empty()) {
                // 查询结果
                displayQueryResults(result);
            } else {
                // DDL/DML结果
                displaySuccess(result.message, result.affectedRows);
            }
        } else {
            displayError(result.message);
        }
        
    } catch (const std::exception& e) {
        displayError("SQL execution error: " + std::string(e.what()));
    }
}

void REPL::handleMetaCommand(const std::string& command) {
    if (command == ".help" || command == ".h") {
        showHelp();
    } else if (command == ".tables") {
        showTables();
    } else if (command.substr(0, 7) == ".schema") {
        std::string tableName = command.length() > 8 ? command.substr(8) : "";
        showSchema(tableName);
    } else if (command == ".history") {
        showHistory();
    } else if (command == ".clear") {
        clearScreen();
    } else if (command == ".stats") {
        showStats();
    } else if (command == ".save") {
        saveDatabase();
    } else if (command == ".version") {
        showVersion();
    } else {
        displayError("Unknown command: " + command + ". Type '.help' for help.");
    }
}

void REPL::showHelp() {
    std::cout << std::endl;
    std::cout << "MiniDB Help:" << std::endl;
    std::cout << "============" << std::endl;
    std::cout << std::endl;
    std::cout << "SQL Commands:" << std::endl;
    std::cout << "  CREATE TABLE name (col1 TYPE, col2 TYPE, ...);" << std::endl;
    std::cout << "  INSERT INTO table VALUES (val1, val2, ...);" << std::endl;
    std::cout << "  SELECT * FROM table [WHERE condition];" << std::endl;
    std::cout << "  DELETE FROM table [WHERE condition];" << std::endl;
    std::cout << std::endl;
    std::cout << "Meta Commands:" << std::endl;
    std::cout << "  .help          - Show this help message" << std::endl;
    std::cout << "  .tables        - List all tables" << std::endl;
    std::cout << "  .schema [table]- Show table schema" << std::endl;
    std::cout << "  .history       - Show command history" << std::endl;
    std::cout << "  .clear         - Clear screen" << std::endl;
    std::cout << "  .stats         - Show database statistics" << std::endl;
    std::cout << "  .save          - Save database to disk" << std::endl;
    std::cout << "  .version       - Show version information" << std::endl;
    std::cout << "  exit           - Exit the database" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  CREATE TABLE users (id INT, name STRING, age INT);" << std::endl;
    std::cout << "  INSERT INTO users VALUES (1, 'Alice', 25);" << std::endl;
    std::cout << "  SELECT name, age FROM users WHERE age > 20;" << std::endl;
    std::cout << std::endl;
}

void REPL::showTables() {
    auto tableNames = storageEngine_->getAllTableNames();
    
    if (tableNames.empty()) {
        std::cout << "No tables found." << std::endl;
        return;
    }
    
    std::cout << std::endl;
    std::cout << "Tables in database:" << std::endl;
    std::cout << "===================" << std::endl;
    
    for (const auto& tableName : tableNames) {
        auto table = storageEngine_->getTable(tableName);
        if (table) {
            std::cout << "  " << tableName << " (" << table->getRowCount() << " rows)" << std::endl;
        }
    }
    std::cout << std::endl;
}

void REPL::showSchema(const std::string& tableName) {
    if (tableName.empty()) {
        // 显示所有表的schema
        auto tableNames = storageEngine_->getAllTableNames();
        
        if (tableNames.empty()) {
            std::cout << "No tables found." << std::endl;
            return;
        }
        
        for (const auto& name : tableNames) {
            showSchema(name);
        }
        return;
    }
    
    auto table = storageEngine_->getTable(tableName);
    if (!table) {
        displayError("Table '" + tableName + "' does not exist");
        return;
    }
    
    std::cout << std::endl;
    std::cout << "Schema for table '" << tableName << "':" << std::endl;
    std::cout << "===============================" << std::endl;
    
    const auto& columns = table->getColumns();
    
    // 创建表格数据
    std::vector<std::vector<std::string>> data;
    std::vector<std::string> headers = {"Column", "Type", "Constraints", "Index"};
    
    for (size_t i = 0; i < columns.size(); ++i) {
        std::vector<std::string> row;
        row.push_back(columns[i].name);
        
        std::string typeStr;
        switch (columns[i].type) {
            case DataType::INT: typeStr = "INT"; break;
            case DataType::STRING: typeStr = "STRING"; break;
            case DataType::DOUBLE: typeStr = "DOUBLE"; break;
            default: typeStr = "UNKNOWN"; break;
        }
        row.push_back(typeStr);
        
        // 构建约束字符串
        std::string constraints;
        if (columns[i].isPrimaryKey) {
            constraints += "PRIMARY KEY";
        }
        if (columns[i].isNotNull) {
            if (!constraints.empty()) constraints += ", ";
            constraints += "NOT NULL";
        }
        if (constraints.empty()) {
            constraints = "-";
        }
        row.push_back(constraints);
        
        row.push_back(std::to_string(i));
        
        data.push_back(row);
    }
    
    std::cout << formatTable(data, headers) << std::endl;
    std::cout << "Total: " << columns.size() << " columns, " << table->getRowCount() << " rows" << std::endl;
    std::cout << std::endl;
}

void REPL::showHistory() {
    if (commandHistory_.empty()) {
        std::cout << "No command history." << std::endl;
        return;
    }
    
    std::cout << std::endl;
    std::cout << "Command History:" << std::endl;
    std::cout << "===============" << std::endl;
    
    for (size_t i = 0; i < commandHistory_.size(); ++i) {
        std::cout << std::setw(3) << (i + 1) << ": " << commandHistory_[i] << std::endl;
    }
    std::cout << std::endl;
}

void REPL::clearScreen() {
#ifdef _WIN32
    system("cls");
#else
    system("clear");
#endif
}

void REPL::showStats() {
    std::cout << std::endl;
    std::cout << "Database Statistics:" << std::endl;
    std::cout << "===================" << std::endl;
    std::cout << "Database path: " << dbPath_ << std::endl;
    
    auto tableNames = storageEngine_->getAllTableNames();
    std::cout << "Total tables: " << tableNames.size() << std::endl;
    
    size_t totalRows = 0;
    for (const auto& tableName : tableNames) {
        auto table = storageEngine_->getTable(tableName);
        if (table) {
            totalRows += table->getRowCount();
        }
    }
    std::cout << "Total rows: " << totalRows << std::endl;
    
    // 显示执行引擎统计
    const auto& engineStats = executionEngine_->getStats();
    std::cout << "Commands executed: " << engineStats.totalStatements << std::endl;
    std::cout << "Successful commands: " << engineStats.successfulStatements << std::endl;
    std::cout << "Failed commands: " << engineStats.failedStatements << std::endl;
    
    if (engineStats.totalStatements > 0) {
        double successRate = (double)engineStats.successfulStatements / engineStats.totalStatements * 100.0;
        std::cout << "Success rate: " << std::fixed << std::setprecision(1) << successRate << "%" << std::endl;
    }
    
    std::cout << std::endl;
}

void REPL::saveDatabase() {
    std::cout << "Saving database..." << std::endl;
    bool success = storageEngine_->saveToStorage();
    if (success) {
        displaySuccess("Database saved successfully");
    } else {
        displayError("Failed to save database");
    }
}

void REPL::showVersion() {
    std::cout << std::endl;
    std::cout << "MiniDB Version 1.0" << std::endl;
    std::cout << "==================" << std::endl;
    std::cout << "A simple relational database management system" << std::endl;
    std::cout << "Built with C++17" << std::endl;
    std::cout << std::endl;
    std::cout << "Features:" << std::endl;
    std::cout << "- SQL DDL (CREATE TABLE)" << std::endl;
    std::cout << "- SQL DML (INSERT, SELECT, DELETE)" << std::endl;
    std::cout << "- Page-based storage system" << std::endl;
    std::cout << "- B+ tree indexing" << std::endl;
    std::cout << "- Buffer pool management (LRU)" << std::endl;
    std::cout << "- SQL lexical and syntax analysis" << std::endl;
    std::cout << "- Semantic analysis with catalog" << std::endl;
    std::cout << "- Query execution engine" << std::endl;
    std::cout << std::endl;
}

void REPL::displayQueryResults(const ExecutionResult& result) {
    if (result.rows.empty()) {
        std::cout << "(0 rows)" << std::endl;
        return;
    }
    
    // 获取输出模式（假设所有行有相同的列数）
    size_t columnCount = result.rows[0].getFieldCount();
    
    // 创建表格数据
    std::vector<std::vector<std::string>> data;
    std::vector<std::string> headers;
    
    // 生成列标题
    if (!result.columnInfo.empty()) {
        // 使用实际的列名
        for (const auto& colInfo : result.columnInfo) {
            headers.push_back(colInfo.name);
        }
    } else {
        // 如果没有列信息，使用默认名称
        for (size_t i = 0; i < columnCount; ++i) {
            headers.push_back("col" + std::to_string(i + 1));
        }
    }
    
    // 转换行数据
    for (const auto& row : result.rows) {
        std::vector<std::string> rowData;
        for (size_t i = 0; i < row.getFieldCount(); ++i) {
            rowData.push_back(valueToString(row.getValue(i)));
        }
        data.push_back(rowData);
    }
    
    std::cout << std::endl;
    std::cout << formatTable(data, headers) << std::endl;
    std::cout << "(" << result.rows.size() << " row" << (result.rows.size() != 1 ? "s" : "") << ")" << std::endl;
    std::cout << std::endl;
}

void REPL::displayError(const std::string& error) {
    std::cout << "Error: " << error << std::endl;
}

void REPL::displaySuccess(const std::string& message, size_t affectedRows) {
    std::cout << "Success: " << message;
    if (affectedRows > 0) {
        std::cout << " (" << affectedRows << " row" << (affectedRows != 1 ? "s" : "") << " affected)";
    }
    std::cout << std::endl;
}

std::string REPL::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) {
        return "";
    }
    
    size_t end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

bool REPL::isMetaCommand(const std::string& input) {
    return !input.empty() && input[0] == '.';
}

std::string REPL::formatTable(const std::vector<std::vector<std::string>>& data, const std::vector<std::string>& headers) {
    if (data.empty() || headers.empty()) {
        return "";
    }
    
    // 计算每列的最大宽度
    std::vector<size_t> columnWidths(headers.size(), 0);
    
    // 检查标题宽度
    for (size_t i = 0; i < headers.size(); ++i) {
        columnWidths[i] = std::max(columnWidths[i], headers[i].length());
    }
    
    // 检查数据宽度
    for (const auto& row : data) {
        for (size_t i = 0; i < std::min(row.size(), columnWidths.size()); ++i) {
            columnWidths[i] = std::max(columnWidths[i], row[i].length());
        }
    }
    
    std::stringstream ss;
    
    // 生成分隔线
    auto generateSeparator = [&columnWidths]() {
        std::string sep = "+";
        for (size_t width : columnWidths) {
            sep += std::string(width + 2, '-') + "+";
        }
        return sep;
    };
    
    // 顶部分隔线
    ss << generateSeparator() << std::endl;
    
    // 标题行
    ss << "|";
    for (size_t i = 0; i < headers.size(); ++i) {
        ss << " " << std::left << std::setw(columnWidths[i]) << headers[i] << " |";
    }
    ss << std::endl;
    
    // 标题下分隔线
    ss << generateSeparator() << std::endl;
    
    // 数据行
    for (const auto& row : data) {
        ss << "|";
        for (size_t i = 0; i < headers.size(); ++i) {
            std::string cellValue = (i < row.size()) ? row[i] : "";
            ss << " " << std::left << std::setw(columnWidths[i]) << cellValue << " |";
        }
        ss << std::endl;
    }
    
    // 底部分隔线
    ss << generateSeparator();
    
    return ss.str();
}

std::string REPL::valueToString(const Value& value) {
    if (std::holds_alternative<int>(value)) {
        return std::to_string(std::get<int>(value));
    } else if (std::holds_alternative<double>(value)) {
        return std::to_string(std::get<double>(value));
    } else if (std::holds_alternative<std::string>(value)) {
        return std::get<std::string>(value);
    }
    return "NULL";
}