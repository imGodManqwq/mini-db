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
        
        // 测试微秒显示是否正常
        std::cout << "Testing microsecond display: 1000 us = 1.00 ms" << std::endl;
        
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
        const int RECORD_COUNT = 2000;  // 插入记录以更好展示索引优势，避免缓冲池溢出
        
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
                auto rebuild_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_rebuild - start_rebuild);
                std::cout << "v Index rebuilding completed in " << rebuild_duration.count() << " us (" 
                          << std::fixed << std::setprecision(2) << (rebuild_duration.count() / 1000.0) << " ms)" << std::endl;
            }
        } else {
            std::cout << "Need to insert " << (RECORD_COUNT - existingRecords) << " more records" << std::endl;
            
            std::vector<std::string> departments = {"Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "IT", "Legal", "Research", "Support"};
            std::vector<std::string> names = {"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"};
            
            auto start_insert = std::chrono::high_resolution_clock::now();
            int insertCount = 0;
            
            std::cout << "Using normal INSERT statements to ensure index consistency..." << std::endl;
            
            // 使用正常INSERT确保索引一致性
            for (int i = existingRecords + 1; i <= RECORD_COUNT; ++i) {
                // 构建INSERT SQL语句
                std::string name = names[i % names.size()] + std::to_string(i);
                std::string department = departments[i % departments.size()];
                int age = 20 + (i % 45);
                double salary = 30000.0 + (i * 5.0);
                
                std::ostringstream insertSQL;
                insertSQL << "INSERT INTO employees VALUES (" 
                          << i << ", '" << name << "', '" << department << "', " 
                          << age << ", " << salary << ");";
                
                // 执行INSERT语句
                Parser parser(insertSQL.str());
                auto stmt = parser.parseStatement();
                if (stmt) {
                    auto result = engine.executeStatement(stmt.get());
                    if (result.isSuccess()) {
                        insertCount++;
                    } else {
                        std::cout << "x Failed to insert record " << i << ": " << result.message << std::endl;
                    }
                } else {
                    std::cout << "x Failed to parse INSERT for record " << i << std::endl;
                }
                
                // 每1000条记录显示一次进度
                if (i % 1000 == 0) {
                    std::cout << "Inserted " << insertCount << " records..." << std::endl;
                }
            }


            // 插入一个特殊的100岁雇员记录，作为唯一的100岁雇员
            std::cout << "Inserting special 100-year-old employee..." << std::endl;
            std::ostringstream specialInsertSQL;
            specialInsertSQL << "INSERT INTO employees VALUES (" 
                            << (RECORD_COUNT + 1) << ", 'Centennial', 'Special', 100, 100000.0);";
            
            Parser specialParser(specialInsertSQL.str());
            auto specialStmt = specialParser.parseStatement();
            if (specialStmt) {
                auto result = engine.executeStatement(specialStmt.get());
                if (result.isSuccess()) {
                    insertCount++;
                    std::cout << "v Special 100-year-old employee inserted successfully" << std::endl;
                } else {
                    std::cout << "x Failed to insert special employee: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Failed to parse special INSERT" << std::endl;
            }

            
            auto end_insert = std::chrono::high_resolution_clock::now();
            auto insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_insert - start_insert);
            std::cout << "v Inserted " << insertCount << " new records in " << insert_duration.count() << " us (" 
                      << std::fixed << std::setprecision(2) << (insert_duration.count() / 1000.0) << " ms)" << std::endl;
            
            // 正常INSERT已经自动维护了所有索引，无需重建
            std::cout << "\nAll indexes automatically maintained during INSERT operations" << std::endl;
            
            // 可选：刷新脏页面到磁盘
            std::cout << "\nFlushing pages to disk..." << std::endl;
            auto start_flush = std::chrono::high_resolution_clock::now();
            storage.flushAllPages();
            auto end_flush = std::chrono::high_resolution_clock::now();
            auto flush_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_flush - start_flush);
            std::cout << "v Page flushing completed in " << flush_duration.count() << " us (" 
                      << std::fixed << std::setprecision(2) << (flush_duration.count() / 1000.0) << " ms)" << std::endl;
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
            {"Rare age lookup", "SELECT * FROM employees WHERE age = 100;"},
        };
        
        std::vector<long long> no_index_times_us;  // 微秒
        
        for (const auto& query : queries) {
            std::cout << "\n--- " << query.first << " (No Index) ---" << std::endl;
            std::cout << "SQL: " << query.second << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            Parser parser(query.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                
                if (result.isSuccess()) {
                    std::cout << "v Found " << result.rows.size() << " records in " << duration.count() << " us" << std::endl;
                    no_index_times_us.push_back(duration.count());
                } else {
                    std::cout << "x Query failed: " << result.message << std::endl;
                    no_index_times_us.push_back(-1);
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
                no_index_times_us.push_back(-1);
            }
        }
        
        // 5. 检查并创建索引
        std::cout << "\n5. Checking and creating indexes..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> indexesToCreate = {
            {"idx_age", "CREATE INDEX idx_age ON employees(age);"}
            // {"idx_department", "CREATE INDEX idx_department ON employees(department);"},
            // {"idx_salary", "CREATE INDEX idx_salary ON employees(salary);"}
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
        
        std::vector<long long> with_index_times_us;  // 微秒
        
        for (const auto& query : queries) {
            std::cout << "\n--- " << query.first << " (With Index) ---" << std::endl;
            std::cout << "SQL: " << query.second << std::endl;
            
            auto start = std::chrono::high_resolution_clock::now();
            
            Parser parser(query.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                
                if (result.isSuccess()) {
                    std::cout << "v Found " << result.rows.size() << " records in " << duration.count() << " us" << std::endl;
                    with_index_times_us.push_back(duration.count());
                } else {
                    std::cout << "x Query failed: " << result.message << std::endl;
                    with_index_times_us.push_back(-1);
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
                with_index_times_us.push_back(-1);
            }
        }
        
        // 7. 性能对比总结
        std::cout << "\n7. Performance Comparison Summary" << std::endl;
        std::cout << "========================================================" << std::endl;
        std::cout << std::left << std::setw(25) << "Query Type" 
                  << std::setw(18) << "No Index (us)" 
                  << std::setw(18) << "With Index (us)" 
                  << std::setw(15) << "Speedup" << std::endl;
        std::cout << std::string(76, '-') << std::endl;
        
        for (size_t i = 0; i < queries.size(); ++i) {
            std::cout << std::left << std::setw(25) << queries[i].first;
            
            if (no_index_times_us[i] >= 0 && with_index_times_us[i] >= 0) {
                double speedup = with_index_times_us[i] > 0 ? 
                    static_cast<double>(no_index_times_us[i]) / with_index_times_us[i] : 1.0;
                std::cout << std::setw(18) << no_index_times_us[i]
                          << std::setw(18) << with_index_times_us[i]
                          << std::fixed << std::setprecision(2) << speedup << "x";
            } else {
                std::string noIndexStr = no_index_times_us[i] >= 0 ? std::to_string(no_index_times_us[i]) : "FAILED";
                std::string withIndexStr = with_index_times_us[i] >= 0 ? std::to_string(with_index_times_us[i]) : "FAILED";
                std::cout << std::setw(18) << noIndexStr
                          << std::setw(18) << withIndexStr
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
    std::cout << "Please enter your choice (1-2): ";
    
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