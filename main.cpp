#include <iostream>
#include <filesystem>
#include "./include/cli/REPL.h"
#include "./include/storage/StorageEngine.h"
#include "./include/storage/Row.h"
#include "./include/storage/Table.h"
#include "./include/storage/BufferPool.h"
#include "./include/storage/BPlusTree.h"
#include "./include/storage/IndexManager.h"
#include "./include/parser/Lexer.h"
#include "./include/parser/Token.h"
#include "./include/parser/Parser.h"
#include "./include/parser/AST.h"
#include "./include/parser/Catalog.h"
#include "./include/parser/SemanticAnalyzer.h"
#include "./include/executor/ExecutionEngine.h"
#include <iomanip>
#include <sstream>

// 美化显示查询结果的函数
void printQueryResult(const ExecutionResult& result) {
    if (!result.isSuccess()) {
        std::cout << "x Query failed: " << result.message << std::endl;
        return;
    }

    if (result.rows.empty()) {
        std::cout << "v Query successful. No rows returned." << std::endl;
        return;
    }

    std::cout << "v Query successful. Found " << result.rows.size() << " rows:" << std::endl;

    // 如果有列信息，显示表格格式
    if (!result.columnInfo.empty()) {
        // 计算每列的最大宽度
        std::vector<size_t> columnWidths(result.columnInfo.size());
        
        // 初始化为列名长度
        for (size_t i = 0; i < result.columnInfo.size(); ++i) {
            columnWidths[i] = result.columnInfo[i].name.length();
        }
        
        // 检查数据行的长度
        for (const auto& row : result.rows) {
            for (size_t i = 0; i < row.getFieldCount() && i < columnWidths.size(); ++i) {
                std::string valueStr;
                std::visit([&valueStr](const auto& v) {
                    std::ostringstream oss;
                    oss << v;
                    valueStr = oss.str();
                }, row.getValue(i));
                columnWidths[i] = std::max(columnWidths[i], valueStr.length());
            }
        }
        
        // 显示表头
        std::cout << "  ";
        for (size_t i = 0; i < result.columnInfo.size(); ++i) {
            if (i > 0) std::cout << " | ";
            std::cout << std::left << std::setw(columnWidths[i]) << result.columnInfo[i].name;
        }
        std::cout << std::endl;
        
        // 显示分隔线
        std::cout << "  ";
        for (size_t i = 0; i < result.columnInfo.size(); ++i) {
            if (i > 0) std::cout << "-+-";
            std::cout << std::string(columnWidths[i], '-');
        }
        std::cout << std::endl;
        
        // 显示数据行
        for (const auto& row : result.rows) {
            std::cout << "  ";
            for (size_t i = 0; i < row.getFieldCount() && i < columnWidths.size(); ++i) {
                if (i > 0) std::cout << " | ";
                std::string valueStr;
                std::visit([&valueStr](const auto& v) {
                    std::ostringstream oss;
                    oss << v;
                    valueStr = oss.str();
                }, row.getValue(i));
                std::cout << std::left << std::setw(columnWidths[i]) << valueStr;
            }
            std::cout << std::endl;
        }
    } else {
        // 如果没有列信息，使用原来的显示方式
        for (size_t i = 0; i < result.rows.size(); ++i) {
            std::cout << "  Row " << (i + 1) << ": " << result.rows[i].toString() << std::endl;
        }
    }
}


void testIndexPerformance() {
    std::cout << "=== Starting Index Performance Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 检查是否需要清理数据库（只有在用户明确需要时才清理）
        bool cleanDatabase = false; // 设置为false以保留现有数据
        if (cleanDatabase) {
            std::cout << "Cleaning existing database..." << std::endl;
            std::filesystem::remove_all("./performance_test_db");
        }
        
        StorageEngine storage("./performance_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 检查并创建测试表
        std::cout << "\n2. Checking and creating test table..." << std::endl;
        
        bool tableExists = catalog->tableExists("employees");
        if (tableExists) {
            std::cout << "v Table 'employees' already exists, reusing it" << std::endl;
        } else {
            std::string createSQL = "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, department STRING, age INT, salary DOUBLE);";
            std::cout << "Creating table: " << createSQL << std::endl;
            
            Parser parser1(createSQL);
            auto stmt1 = parser1.parseStatement();
            if (stmt1) {
                auto result = engine.executeStatement(stmt1.get());
                std::cout << (result.isSuccess() ? "v Table created successfully" : ("x Failed: " + result.message)) << std::endl;
            }
        }
        
        // 3. 检查并插入大量测试数据
        std::cout << "\n3. Checking and inserting large dataset..." << std::endl;
        const int RECORD_COUNT = 10000;  // 插入1万条记录以更好展示索引优势，避免缓冲池溢出
        
        // 检查表中现有数据量
        std::string countSQL = "SELECT COUNT(*) FROM employees;";
        Parser countParser(countSQL);
        auto countStmt = countParser.parseStatement();
        int existingRecords = 0;
        
        if (countStmt) {
            auto result = engine.executeStatement(countStmt.get());
            if (result.isSuccess() && !result.rows.empty()) {
                // 获取COUNT(*)的结果
                auto countValue = result.rows[0].getValue(0);
                if (std::holds_alternative<int>(countValue)) {
                    existingRecords = std::get<int>(countValue);
                }
            }
        }
        
        std::cout << "Found " << existingRecords << " existing records in table" << std::endl;
        
        if (existingRecords >= RECORD_COUNT) {
            std::cout << "v Sufficient data already exists (" << existingRecords << " >= " << RECORD_COUNT << "), skipping insertion and index rebuild" << std::endl;
            
            // 检查是否需要重建主键索引（如果索引不完整的话）
            bool needRebuild = false;
            try {
                // 简单测试主键索引是否工作正常
                auto testResults = storage.searchByColumn("employees", "id", Value(1));
                if (testResults.empty()) {
                    needRebuild = true;
                    std::cout << "Primary key index appears to be incomplete, will rebuild" << std::endl;
                }
            } catch (...) {
                needRebuild = true;
                std::cout << "Primary key index test failed, will rebuild" << std::endl;
            }
            
            if (needRebuild) {
                std::cout << "\nRebuilding primary key index..." << std::endl;
                auto start_rebuild = std::chrono::high_resolution_clock::now();
                storage.rebuildTableIndexes("employees");
                auto end_rebuild = std::chrono::high_resolution_clock::now();
                auto rebuild_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_rebuild - start_rebuild);
                std::cout << "v Index rebuilding completed in " << rebuild_duration.count() << " ms" << std::endl;
            }
        } else {
            std::cout << "Need to insert " << (RECORD_COUNT - existingRecords) << " more records" << std::endl;
            
            std::vector<std::string> departments = {"Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "IT", "Legal", "Research", "Support"};
            std::vector<std::string> names = {"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"};
            
            auto start_insert = std::chrono::high_resolution_clock::now();
            int insertCount = 0;
            
            // 使用批量插入优化性能
            const int BATCH_SIZE = 1000;
            std::vector<std::vector<Value>> batchData;
            batchData.reserve(BATCH_SIZE);
            
            for (int i = existingRecords + 1; i <= RECORD_COUNT; ++i) {
                // 直接构建数据行，避免SQL解析开销
                std::vector<Value> rowData = {
                    Value(i),  // id
                    Value(names[i % names.size()] + std::to_string(i)),  // name
                    Value(departments[i % departments.size()]),  // department
                    Value(20 + (i % 45)),  // age: 20-64
                    Value(30000.0 + (i * 5.0))  // salary
                };
                
                batchData.push_back(std::move(rowData));
                
                // 当批次满了或者是最后一批时，执行快速批量插入（跳过索引更新）
                if (batchData.size() == BATCH_SIZE || i == RECORD_COUNT) {
                    size_t batchSuccess = storage.fastBatchInsertRows("employees", batchData);
                    insertCount += batchSuccess;
                    
                    if (batchSuccess < batchData.size()) {
                        std::cout << "x Some records failed in batch. Success: " << batchSuccess << "/" << batchData.size() << std::endl;
                    }
                    
                    // 显示进度
                    std::cout << "Inserted " << insertCount << " records..." << std::endl;
                    batchData.clear();
                }
            }
            
            auto end_insert = std::chrono::high_resolution_clock::now();
            auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_insert - start_insert);
            std::cout << "v Inserted " << insertCount << " new records in " << insert_duration.count() << " ms" << std::endl;
            
            // 强制写入所有脏页面
            std::cout << "\nFlushing pages to disk..." << std::endl;
            auto start_flush = std::chrono::high_resolution_clock::now();
            storage.flushAllPages();
            auto end_flush = std::chrono::high_resolution_clock::now();
            auto flush_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_flush - start_flush);
            std::cout << "v Page flushing completed in " << flush_duration.count() << " ms" << std::endl;
            
            // 重建主键索引（快速插入跳过了索引更新）
            std::cout << "\nRebuilding primary key index..." << std::endl;
            auto start_rebuild = std::chrono::high_resolution_clock::now();
            storage.rebuildTableIndexes("employees");
            auto end_rebuild = std::chrono::high_resolution_clock::now();
            auto rebuild_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_rebuild - start_rebuild);
            std::cout << "v Index rebuilding completed in " << rebuild_duration.count() << " ms" << std::endl;
        }
        
        // 4. 测试无索引的查询性能（临时删除非主键索引）
        std::cout << "\n4. Testing query performance WITHOUT index..." << std::endl;
        
        // 临时删除非主键索引以进行公平对比
        std::vector<std::string> indexesToDrop = {"idx_age", "idx_department", "idx_salary"};
        for (const auto& indexName : indexesToDrop) {
            try {
                bool dropped = storage.dropIndex(indexName);
                if (dropped) {
                    std::cout << "Temporarily dropped index: " << indexName << std::endl;
                }
            } catch (...) {
                // 索引可能不存在，忽略错误
            }
        }
        
        std::vector<std::pair<std::string, std::string>> queries = {
            {"Specific age lookup", "SELECT * FROM employees WHERE age = 35;"},
            {"Department lookup", "SELECT * FROM employees WHERE department = 'Engineering';"},
            {"High salary range", "SELECT * FROM employees WHERE salary > 70000;"},
            {"Rare age lookup", "SELECT * FROM employees WHERE age = 63;"},  // 更少的记录
            {"IT department", "SELECT * FROM employees WHERE department = 'IT';"}
        };
        
        std::vector<long long> no_index_times;
        
        for (const auto& query : queries) {
            std::cout << "\n--- " << query.first << " (No Index) ---" << std::endl;
            std::cout << "SQL: " << query.second << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            Parser parser(query.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                if (result.isSuccess()) {
                    std::cout << "v Found " << result.rows.size() << " records in " << duration.count() << " ms" << std::endl;
                    no_index_times.push_back(duration.count());
                } else {
                    std::cout << "x Query failed: " << result.message << std::endl;
                    no_index_times.push_back(-1);
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
                no_index_times.push_back(-1);
            }
        }
        
        // 5. 检查并创建索引
        std::cout << "\n5. Checking and creating indexes..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> indexesToCreate = {
            {"idx_age", "CREATE INDEX idx_age ON employees(age);"},
            {"idx_department", "CREATE INDEX idx_department ON employees(department);"},
            {"idx_salary", "CREATE INDEX idx_salary ON employees(salary);"}
        };
        
        for (const auto& indexInfo : indexesToCreate) {
            const std::string& indexName = indexInfo.first;
            const std::string& createSQL = indexInfo.second;
            
            // 首先检查索引是否已存在
            bool indexExists = storage.indexExists(indexName);
            if (indexExists) {
                std::cout << "v Index '" << indexName << "' already exists, skipping creation" << std::endl;
            } else {
                std::cout << "Creating: " << createSQL << std::endl;
                Parser parser(createSQL);
                auto stmt = parser.parseStatement();
                if (stmt) {
                    auto result = engine.executeStatement(stmt.get());
                    if (result.isSuccess()) {
                        std::cout << "v Index '" << indexName << "' created successfully" << std::endl;
                    } else {
                        std::cout << "x Failed to create index '" << indexName << "': " << result.message << std::endl;
                    }
                }
            }
        }
        
        // 6. 测试有索引的查询性能
        std::cout << "\n6. Testing query performance WITH index..." << std::endl;
        
        std::vector<long long> with_index_times;
        
        for (const auto& query : queries) {
            std::cout << "\n--- " << query.first << " (With Index) ---" << std::endl;
            std::cout << "SQL: " << query.second << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            Parser parser(query.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                if (result.isSuccess()) {
                    std::cout << "v Found " << result.rows.size() << " records in " << duration.count() << " ms" << std::endl;
                    with_index_times.push_back(duration.count());
                } else {
                    std::cout << "x Query failed: " << result.message << std::endl;
                    with_index_times.push_back(-1);
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
                with_index_times.push_back(-1);
            }
        }
        
        // 7. 性能对比总结
        std::cout << "\n7. Performance Comparison Summary" << std::endl;
        std::cout << "================================================" << std::endl;
        std::cout << std::left << std::setw(25) << "Query Type" 
                  << std::setw(15) << "No Index (ms)" 
                  << std::setw(15) << "With Index (ms)" 
                  << std::setw(15) << "Speedup" << std::endl;
        std::cout << std::string(70, '-') << std::endl;
        
        for (size_t i = 0; i < queries.size(); ++i) {
            std::cout << std::left << std::setw(25) << queries[i].first;
            
            if (no_index_times[i] >= 0 && with_index_times[i] >= 0) {
                double speedup = with_index_times[i] > 0 ? 
                    static_cast<double>(no_index_times[i]) / with_index_times[i] : 1.0;
                std::cout << std::setw(15) << no_index_times[i]
                          << std::setw(15) << with_index_times[i]
                          << std::fixed << std::setprecision(2) << speedup << "x";
            } else {
                std::string noIndexStr = no_index_times[i] >= 0 ? std::to_string(no_index_times[i]) : "FAILED";
                std::string withIndexStr = with_index_times[i] >= 0 ? std::to_string(with_index_times[i]) : "FAILED";
                std::cout << std::setw(15) << noIndexStr
                          << std::setw(15) << withIndexStr
                          << "N/A";
            }
            std::cout << std::endl;
        }
        
        // 8. 显示索引统计信息
        std::cout << "\n8. Index Statistics" << std::endl;
        storage.printIndexInfo();
        
        // 9. 内存使用和存储统计
        std::cout << "\n9. Storage Statistics" << std::endl;
        std::cout << "Total records: " << RECORD_COUNT << std::endl;
        std::cout << "Tables: 1" << std::endl;
        std::cout << "Indexes: 4 (including primary key)" << std::endl;
        
        std::cout << "\n=== Index Performance Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during performance testing: " << e.what() << std::endl;
    }
}


int main() {
    std::cout << "MiniDB Started" << std::endl;
    std::cout << "Choose mode:" << std::endl;
    std::cout << "1. Test Index Performance (Speed comparison with/without indexes)" << std::endl;
    std::cout << "2. Start REPL Interactive Mode" << std::endl;
    std::cout << "Please enter your choice (1-19): ";
    
    int choice;
    std::cin >> choice;
    
    if (choice == 1) {
        testIndexPerformance();
    } else {
        std::cin.ignore(); // 清除输入缓冲
        REPL repl;
        repl.run();
    }
    
    return 0;
}