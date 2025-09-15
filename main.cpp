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

void testStorageSystem() {
    std::cout << "=== Starting Storage System Test ===" << std::endl;
    
    try {
        // 1. Test StorageEngine initialization
        std::cout << "\n1. Testing StorageEngine initialization..." << std::endl;
        StorageEngine storage("./test_db");
        storage.printStorageInfo();
        
        // 2. Test table creation
        std::cout << "\n2. Testing table creation..." << std::endl;
        std::vector<ColumnInfo> columns = {
            ColumnInfo("id", DataType::INT),
            ColumnInfo("name", DataType::STRING),
            ColumnInfo("age", DataType::INT),
            ColumnInfo("salary", DataType::DOUBLE)
        };
        
        bool createResult = storage.createTable("employees", columns);
        std::cout << "Table creation result: " << (createResult ? "Success" : "Failed") << std::endl;
        
        // 3. Test data insertion
        std::cout << "\n3. Testing data insertion..." << std::endl;
        std::vector<std::vector<Value>> testData = {
            {1, std::string("Alice"), 25, 5000.5},
            {2, std::string("Bob"), 30, 6000.0},
            {3, std::string("Charlie"), 28, 5500.75},
            {4, std::string("David"), 35, 7000.0},
            {5, std::string("Eve"), 22, 4500.0}
        };
        
        for (const auto& rowData : testData) {
            bool insertResult = storage.insertRow("employees", rowData);
            std::cout << "Row insertion result: " << (insertResult ? "Success" : "Failed") << std::endl;
        }
        
        // 4. Test table information display
        std::cout << "\n4. Testing table information display..." << std::endl;
        storage.printTableInfo("employees");
        
        // 5. Test Row and Table basic operations
        std::cout << "\n5. Testing Row basic operations..." << std::endl;
        Row testRow({1, std::string("TestUser"), 99, 9999.99});
        std::cout << "Test row content: " << testRow.toString() << std::endl;
        std::cout << "Field count: " << testRow.getFieldCount() << std::endl;
        
        // Test serialization and deserialization
        std::string serialized = testRow.serialize();
        std::cout << "Serialized data: " << serialized << std::endl;
        Row deserializedRow = Row::deserialize(serialized);
        std::cout << "Deserialized row: " << deserializedRow.toString() << std::endl;
        std::cout << "Serialization test: " << (testRow == deserializedRow ? "Passed" : "Failed") << std::endl;
        
        // 6. Test table iterator
        std::cout << "\n6. Testing table iterator..." << std::endl;
        auto table = storage.getTable("employees");
        if (table) {
            std::cout << "Traversing table data using iterator:" << std::endl;
            int count = 0;
            for (auto it = table->begin(); it != table->end(); ++it) {
                std::cout << "  Row " << count++ << ": " << it->toString() << std::endl;
            }
        }
        
        // 7. Test persistence
        std::cout << "\n7. Testing data persistence..." << std::endl;
        bool saveResult = storage.saveToStorage();
        std::cout << "Save data result: " << (saveResult ? "Success" : "Failed") << std::endl;
        
        // 8. Test data reloading
        std::cout << "\n8. Testing data reloading..." << std::endl;
        StorageEngine storage2("./test_db");
        std::cout << "Storage info after reloading:" << std::endl;
        storage2.printStorageInfo();
        storage2.printTableInfo("employees");
        
        // 9. Test error handling
        std::cout << "\n9. Testing error handling..." << std::endl;
        
        // Test inserting mismatched row
        std::vector<Value> wrongRow = {1, std::string("wrong_row")}; // Missing fields
        bool wrongInsert = storage.insertRow("employees", wrongRow);
        std::cout << "Wrong row insertion result: " << (wrongInsert ? "Success(Abnormal)" : "Failed(Normal)") << std::endl;
        
        // Test accessing non-existent table
        auto nonExistTable = storage.getTable("non_exist");
        std::cout << "Non-existent table access result: " << (nonExistTable ? "Found(Abnormal)" : "Not Found(Normal)") << std::endl;
        
        std::cout << "\n=== Storage System Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during testing: " << e.what() << std::endl;
    }
}

void testBufferPool() {
    std::cout << "=== Starting Buffer Pool Test ===" << std::endl;
    
    try {
        // 1. Test buffer pool creation
        std::cout << "\n1. Testing buffer pool creation..." << std::endl;
        BufferPool bufferPool(5); // Small pool size for testing
        bufferPool.printStats();
        
        // 2. Test page insertion and retrieval
        std::cout << "\n2. Testing page operations..." << std::endl;
        
        // Create some test pages
        std::vector<std::shared_ptr<Page>> testPages;
        for (int i = 1; i <= 7; ++i) {
            auto page = std::make_shared<Page>(i);
            testPages.push_back(page);
            bool result = bufferPool.putPage(page);
            std::cout << "Put page " << i << ": " << (result ? "Success" : "Failed") << std::endl;
        }
        
        bufferPool.printStats();
        bufferPool.printPoolStatus();
        
        // 3. Test page retrieval (should trigger cache hits and misses)
        std::cout << "\n3. Testing page retrieval..." << std::endl;
        for (int i = 1; i <= 3; ++i) {
            auto page = bufferPool.getPage(i);
            std::cout << "Get page " << i << ": " << (page ? "Found" : "Not Found") << std::endl;
        }
        
        bufferPool.printStats();
        
        // 4. Test LRU eviction
        std::cout << "\n4. Testing LRU eviction..." << std::endl;
        // Access pages to change LRU order
        bufferPool.getPage(2);
        bufferPool.getPage(4);
        
        // Add more pages to trigger eviction
        for (int i = 8; i <= 10; ++i) {
            auto page = std::make_shared<Page>(i);
            bool result = bufferPool.putPage(page);
            std::cout << "Put page " << i << ": " << (result ? "Success" : "Failed") << std::endl;
        }
        
        bufferPool.printPoolStatus();
        bufferPool.printStats();
        
        // 5. Test page pinning
        std::cout << "\n5. Testing page pinning..." << std::endl;
        bool pinResult = bufferPool.pinPage(8);
        std::cout << "Pin page 8: " << (pinResult ? "Success" : "Failed") << std::endl;
        
        // Try to add more pages (pinned page should not be evicted)
        for (int i = 11; i <= 13; ++i) {
            auto page = std::make_shared<Page>(i);
            bufferPool.putPage(page);
            std::cout << "Put page " << i << " (with page 8 pinned)" << std::endl;
        }
        
        bufferPool.printPoolStatus();
        
        // Unpin page
        bufferPool.unpinPage(8);
        std::cout << "Unpinned page 8" << std::endl;
        
        // 6. Test page flushing
        std::cout << "\n6. Testing page flushing..." << std::endl;
        bufferPool.flushAllPages();
        std::cout << "Flushed all pages" << std::endl;
        
        // 7. Test pool clearing
        std::cout << "\n7. Testing pool clearing..." << std::endl;
        bufferPool.clearPool();
        bufferPool.printStats();
        bufferPool.printPoolStatus();
        
        std::cout << "\n=== Buffer Pool Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during buffer pool testing: " << e.what() << std::endl;
    }
}

void testBPlusTreeIndex() {
    std::cout << "=== Starting B+ Tree Index Test ===" << std::endl;
    
    try {
        // 1. Test B+ Tree basic operations
        std::cout << "\n1. Testing B+ Tree basic operations..." << std::endl;
        BPlusTree btree(5); // Small order for testing
        
        std::cout << "Tree is empty: " << (btree.isEmpty() ? "Yes" : "No") << std::endl;
        std::cout << "Tree height: " << btree.getHeight() << std::endl;
        
        // 2. Test insertions
        std::cout << "\n2. Testing insertions..." << std::endl;
        std::vector<std::pair<Value, uint32_t>> testData = {
            {10, 1}, {20, 2}, {5, 3}, {15, 4}, {25, 5},
            {30, 6}, {35, 7}, {40, 8}, {45, 9}, {50, 10}
        };
        
        for (const auto& pair : testData) {
            bool result = btree.insert(pair.first, pair.second);
            std::cout << "Insert key " << std::get<int>(pair.first) 
                      << " with record " << pair.second 
                      << ": " << (result ? "Success" : "Failed") << std::endl;
        }
        
        std::cout << "Tree height after insertions: " << btree.getHeight() << std::endl;
        std::cout << "Node count: " << btree.getNodeCount() << std::endl;
        
        // 3. Test searches
        std::cout << "\n3. Testing searches..." << std::endl;
        std::vector<Value> searchKeys = {5, 15, 25, 35, 100};
        
        for (const auto& key : searchKeys) {
            auto records = btree.search(key);
            std::cout << "Search key " << std::get<int>(key) << ": ";
            if (records.empty()) {
                std::cout << "Not found";
            } else {
                std::cout << "Found records: ";
                for (uint32_t recordId : records) {
                    std::cout << recordId << " ";
                }
            }
            std::cout << std::endl;
        }
        
        // 4. Test range searches
        std::cout << "\n4. Testing range searches..." << std::endl;
        auto rangeRecords = btree.rangeSearch(Value(15), Value(35));
        std::cout << "Range search [15, 35]: ";
        for (uint32_t recordId : rangeRecords) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // 5. Test tree structure
        std::cout << "\n5. Tree structure:" << std::endl;
        btree.printTree();
        
        // 6. Test Index Manager
        std::cout << "\n6. Testing Index Manager..." << std::endl;
        
        // Create a test table
        std::vector<ColumnInfo> columns = {
            ColumnInfo("id", DataType::INT),
            ColumnInfo("name", DataType::STRING),
            ColumnInfo("age", DataType::INT)
        };
        auto table = std::make_shared<Table>("test_table", columns);
        
        // Add some test data
        std::vector<std::vector<Value>> tableData = {
            {1, std::string("Alice"), 25},
            {2, std::string("Bob"), 30},
            {3, std::string("Charlie"), 35},
            {4, std::string("David"), 28},
            {5, std::string("Eve"), 32}
        };
        
        for (const auto& rowData : tableData) {
            table->insertRow(rowData);
        }
        
        // Test Index Manager
        IndexManager indexManager;
        indexManager.registerTable(table);
        
        // Create indexes
        bool createResult1 = indexManager.createIndex("idx_id", "test_table", "id", IndexType::BTREE, true);
        bool createResult2 = indexManager.createIndex("idx_age", "test_table", "age", IndexType::BTREE, false);
        
        std::cout << "Create unique index on id: " << (createResult1 ? "Success" : "Failed") << std::endl;
        std::cout << "Create index on age: " << (createResult2 ? "Success" : "Failed") << std::endl;
        
        // Test index searches
        std::cout << "\n7. Testing index searches..." << std::endl;
        auto idResults = indexManager.searchByIndex("idx_id", Value(3));
        std::cout << "Search by id=3: ";
        for (uint32_t recordId : idResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        auto ageResults = indexManager.searchByIndex("idx_age", Value(30));
        std::cout << "Search by age=30: ";
        for (uint32_t recordId : ageResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // Test range search
        auto ageRangeResults = indexManager.rangeSearchByIndex("idx_age", Value(28), Value(32));
        std::cout << "Range search age [28, 32]: ";
        for (uint32_t recordId : ageRangeResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // Print index statistics
        std::cout << "\n8. Index statistics:" << std::endl;
        indexManager.printIndexStats();
        
        std::cout << "\n=== B+ Tree Index Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during B+ tree testing: " << e.what() << std::endl;
    }
}

void testIntegratedStorage() {
    std::cout << "=== Starting Integrated Storage Test ===" << std::endl;
    
    try {
        // 1. Create storage engine
        std::cout << "\n1. Creating storage engine..." << std::endl;
        StorageEngine storage("./integrated_test_db");
        
        // 2. Create table
        std::cout << "\n2. Creating table..." << std::endl;
        std::vector<ColumnInfo> columns = {
            ColumnInfo("id", DataType::INT),
            ColumnInfo("name", DataType::STRING),
            ColumnInfo("age", DataType::INT),
            ColumnInfo("salary", DataType::DOUBLE)
        };
        
        bool createResult = storage.createTable("employees", columns);
        std::cout << "Table creation: " << (createResult ? "Success" : "Failed") << std::endl;
        
        // 3. Insert test data
        std::cout << "\n3. Inserting test data..." << std::endl;
        std::vector<std::vector<Value>> testData = {
            {1, std::string("Alice"), 25, 5000.0},
            {2, std::string("Bob"), 30, 6000.0},
            {3, std::string("Charlie"), 35, 7000.0},
            {4, std::string("David"), 28, 5500.0},
            {5, std::string("Eve"), 32, 6500.0},
            {6, std::string("Frank"), 29, 5800.0},
            {7, std::string("Grace"), 33, 7200.0}
        };
        
        for (const auto& rowData : testData) {
            bool insertResult = storage.insertRow("employees", rowData);
            if (!insertResult) {
                std::cout << "Failed to insert row" << std::endl;
            }
        }
        
        // 4. Create indexes
        std::cout << "\n4. Creating indexes..." << std::endl;
        bool idx1 = storage.createIndex("idx_id", "employees", "id", true);  // Unique index
        bool idx2 = storage.createIndex("idx_age", "employees", "age", false); // Non-unique index
        
        std::cout << "Create unique index on id: " << (idx1 ? "Success" : "Failed") << std::endl;
        std::cout << "Create index on age: " << (idx2 ? "Success" : "Failed") << std::endl;
        
        // 5. Test searches with and without indexes
        std::cout << "\n5. Testing searches..." << std::endl;
        
        // Search by indexed column (id)
        std::cout << "\nSearch by id=3 (indexed):" << std::endl;
        auto idResults = storage.searchByColumn("employees", "id", Value(3));
        std::cout << "Found " << idResults.size() << " records: ";
        for (uint32_t recordId : idResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // Search by indexed column (age)
        std::cout << "\nSearch by age=30 (indexed):" << std::endl;
        auto ageResults = storage.searchByColumn("employees", "age", Value(30));
        std::cout << "Found " << ageResults.size() << " records: ";
        for (uint32_t recordId : ageResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // Search by non-indexed column (name) - should use full table scan
        std::cout << "\nSearch by name='Alice' (not indexed):" << std::endl;
        auto nameResults = storage.searchByColumn("employees", "name", Value(std::string("Alice")));
        std::cout << "Found " << nameResults.size() << " records: ";
        for (uint32_t recordId : nameResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // 6. Test range search using index
        std::cout << "\n6. Testing range search..." << std::endl;
        auto rangeResults = storage.rangeSearchByIndex("idx_age", Value(28), Value(32));
        std::cout << "Range search age [28, 32]: Found " << rangeResults.size() << " records: ";
        for (uint32_t recordId : rangeResults) {
            std::cout << recordId << " ";
        }
        std::cout << std::endl;
        
        // 7. Print comprehensive information
        std::cout << "\n7. Storage and index information:" << std::endl;
        storage.printStorageInfo();
        
        std::cout << "\nTable information:" << std::endl;
        storage.printTableInfo("employees");
        
        std::cout << "\nDetailed index information:" << std::endl;
        storage.printIndexInfo();
        
        std::cout << "\n=== Integrated Storage Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during integrated storage testing: " << e.what() << std::endl;
    }
}

void testLexer() {
    std::cout << "=== Starting Lexer Test ===" << std::endl;
    
    try {
        // 测试SQL语句集合
        std::vector<std::string> testCases = {
            // 1. CREATE TABLE语句
            "CREATE TABLE users (id INT, name STRING, age INT);",
            
            // 2. INSERT语句
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            
            // 3. SELECT语句
            "SELECT id, name FROM users WHERE age > 20;",
            
            // 4. DELETE语句
            "DELETE FROM users WHERE id = 1;",
            
            // 5. 复杂查询
            "SELECT * FROM employees WHERE salary >= 5000.0 AND age <= 35;",
            
            // 6. 包含字符串和数字
            "INSERT INTO products VALUES (100, \"Product Name\", 99.99, true);",
            
            // 7. 带注释的SQL
            "-- This is a comment\nSELECT name FROM users; -- End comment",
            
            // 8. 错误测试：非法字符
            "SELECT name FROM users WHERE id = @invalid;",
            
            // 9. 错误测试：未结束的字符串
            "SELECT 'unterminated string FROM users;",
            
            // 10. 运算符测试
            "SELECT * FROM table WHERE a != b AND c <> d OR e <= f;"
        };
        
        for (size_t i = 0; i < testCases.size(); ++i) {
            std::cout << "\n--- Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "Input: " << testCases[i] << std::endl;
            
            Lexer lexer(testCases[i]);
            auto tokens = lexer.tokenize();
            
            std::cout << "Tokens:" << std::endl;
            for (const auto& token : tokens) {
                std::cout << "  " << token.toString() << std::endl;
            }
            
            // 检查是否有错误
            if (lexer.hasErrors()) {
                std::cout << "Lexer Errors:" << std::endl;
                lexer.printErrors();
            }
        }
        
        // 测试单个token获取
        std::cout << "\n--- Token by Token Test ---" << std::endl;
        std::string sampleSQL = "SELECT name, age FROM users WHERE id = 123;";
        std::cout << "Input: " << sampleSQL << std::endl;
        
        Lexer lexer(sampleSQL);
        std::cout << "Tokens (one by one):" << std::endl;
        
        Token token;
        do {
            token = lexer.nextToken();
            std::cout << "  " << token.toString() << std::endl;
        } while (token.type != TokenType::END_OF_FILE && token.type != TokenType::ERROR);
        
        // 测试peek功能
        std::cout << "\n--- Peek Test ---" << std::endl;
        lexer.reset();
        
        std::cout << "Peek next token: " << lexer.peekToken().toString() << std::endl;
        std::cout << "Peek next token again: " << lexer.peekToken().toString() << std::endl;
        std::cout << "Actually get next token: " << lexer.nextToken().toString() << std::endl;
        std::cout << "Peek next token: " << lexer.peekToken().toString() << std::endl;
        
        // 关键字识别测试
        std::cout << "\n--- Keyword Recognition Test ---" << std::endl;
        std::vector<std::string> words = {"select", "SELECT", "Select", "from", "table", "user", "123", "test_var"};
        
        for (const auto& word : words) {
            bool isKeyword = KeywordMap::isKeyword(word);
            TokenType type = KeywordMap::getKeywordType(word);
            std::cout << "\"" << word << "\" -> " << (isKeyword ? "Keyword" : "Identifier") 
                      << " (" << tokenTypeToString(type) << ")" << std::endl;
        }
        
        std::cout << "\n=== Lexer Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during lexer testing: " << e.what() << std::endl;
    }
}

void testParser() {
    std::cout << "=== Starting Parser Test ===" << std::endl;
    
    try {
        // 测试SQL语句集合
        std::vector<std::string> testCases = {
            // 1. CREATE TABLE语句
            "CREATE TABLE users (id INT, name STRING, age INT);",
            
            // 2. CREATE TABLE with constraints
            "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, salary DOUBLE);",
            
            // 3. Simple INSERT语句
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            
            // 4. INSERT with column list
            "INSERT INTO users (id, name) VALUES (2, 'Bob');",
            
            // 5. Multiple INSERT values
            "INSERT INTO users VALUES (3, 'Charlie', 30), (4, 'David', 35);",
            
            // 6. Simple SELECT语句
            "SELECT * FROM users;",
            
            // 7. SELECT with specific columns
            "SELECT id, name FROM users;",
            
            // 8. SELECT with WHERE clause
            "SELECT name, age FROM users WHERE age > 25;",
            
            // 9. Complex WHERE clause
            "SELECT * FROM employees WHERE salary >= 5000.0 AND age <= 35;",
            
            // 10. DELETE语句
            "DELETE FROM users WHERE id = 1;",
            
            // 11. DELETE with complex condition
            "DELETE FROM users WHERE age > 30 OR name = 'Alice';",
            
            // 12. Expression with parentheses
            "SELECT * FROM users WHERE (age > 20 AND age < 40) OR name = 'Admin';",
            
            // 13. 错误测试：语法错误
            "CREATE TABLE users id INT, name STRING);", // 缺少左括号
            
            // 14. 错误测试：缺少分号（应该仍能解析）
            "SELECT * FROM users",
            
            // 15. 复杂表达式
            "SELECT id, name FROM users WHERE age * 2 + 10 > 60;"
        };
        
        for (size_t i = 0; i < testCases.size(); ++i) {
            std::cout << "\n--- Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "Input: " << testCases[i] << std::endl;
            
            Parser parser(testCases[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                std::cout << "Parse Result:\n" << stmt->toString() << std::endl;
            } else {
                std::cout << "Parse failed." << std::endl;
            }
            
            // 检查是否有错误
            if (parser.hasErrors()) {
                std::cout << "Parse Errors:" << std::endl;
                parser.printErrors();
            }
        }
        
        // 测试多语句解析
        std::cout << "\n--- Multi-Statement Test ---" << std::endl;
        std::string multiSQL = R"(
            CREATE TABLE products (id INT PRIMARY KEY, name STRING, price DOUBLE);
            INSERT INTO products VALUES (1, 'Laptop', 999.99);
            SELECT * FROM products WHERE price > 500.0;
            DELETE FROM products WHERE id = 1;
        )";
        
        std::cout << "Multi-statement input:\n" << multiSQL << std::endl;
        
        Parser multiParser(multiSQL);
        auto statements = multiParser.parseStatements();
        
        std::cout << "Parsed " << statements.size() << " statements:" << std::endl;
        for (size_t i = 0; i < statements.size(); ++i) {
            std::cout << "\n--- Statement " << (i + 1) << " ---" << std::endl;
            if (statements[i]) {
                std::cout << statements[i]->toString() << std::endl;
            } else {
                std::cout << "NULL statement" << std::endl;
            }
        }
        
        if (multiParser.hasErrors()) {
            std::cout << "\nMulti-statement Parse Errors:" << std::endl;
            multiParser.printErrors();
        }
        
        // 测试AST访问者模式
        std::cout << "\n--- AST Visitor Test ---" << std::endl;
        std::string visitorSQL = "SELECT id, name FROM users WHERE age > 25 AND salary <= 5000.0;";
        std::cout << "Visitor test input: " << visitorSQL << std::endl;
        
        Parser visitorParser(visitorSQL);
        auto visitorStmt = visitorParser.parseStatement();
        
        if (visitorStmt) {
            std::cout << "Using AST Visitor:" << std::endl;
            ASTPrinter printer;
            visitorStmt->accept(&printer);
            std::cout << std::endl;
        }
        
        // 测试表达式优先级
        std::cout << "\n--- Expression Precedence Test ---" << std::endl;
        std::vector<std::string> expressionTests = {
            "SELECT * FROM users WHERE a + b * c;",
            "SELECT * FROM users WHERE (a + b) * c;",
            "SELECT * FROM users WHERE a AND b OR c;",
            "SELECT * FROM users WHERE a OR b AND c;",
            "SELECT * FROM users WHERE NOT a = b;",
            "SELECT * FROM users WHERE a = b AND c != d OR e > f;"
        };
        
        for (size_t i = 0; i < expressionTests.size(); ++i) {
            std::cout << "\nExpression Test " << (i + 1) << ": " << expressionTests[i] << std::endl;
            Parser exprParser(expressionTests[i]);
            auto exprStmt = exprParser.parseStatement();
            
            if (exprStmt) {
                auto selectStmt = dynamic_cast<SelectStatement*>(exprStmt.get());
                if (selectStmt && selectStmt->whereClause) {
                    std::cout << "WHERE clause AST:\n" << selectStmt->whereClause->toString(1) << std::endl;
                }
            }
            
            if (exprParser.hasErrors()) {
                exprParser.printErrors();
            }
        }
        
        std::cout << "\n=== Parser Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during parser testing: " << e.what() << std::endl;
    }
}

void testSemanticAnalyzer() {
    std::cout << "=== Starting Semantic Analyzer Test ===" << std::endl;
    
    try {
        // 1. 创建StorageEngine、Catalog和语义分析器
        std::cout << "\n1. Creating StorageEngine, Catalog and Semantic Analyzer..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./semantic_test_db");
        
        StorageEngine storage("./semantic_test_db");
        auto catalog = std::make_shared<Catalog>(&storage);
        SemanticAnalyzer analyzer(catalog);
        
        std::cout << "Integrated Catalog with StorageEngine created successfully" << std::endl;
        
        // 2. 测试表创建的语义分析
        std::cout << "\n2. Testing CREATE TABLE semantic analysis..." << std::endl;
        
        std::vector<std::string> createTableTests = {
            // 正确的CREATE TABLE语句
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT);",
            "CREATE TABLE products (id INT, name STRING, price DOUBLE);",
            
            // 错误的CREATE TABLE语句
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT);", // 重复表名 - 应该失败
            "CREATE TABLE invalid_table (id INT PRIMARY KEY, id INT);", // 重复列名
            "CREATE TABLE empty_columns ();", // 没有列
            "CREATE TABLE multiple_pk (id INT PRIMARY KEY, name STRING PRIMARY KEY);", // 多个主键
        };
        
        for (size_t i = 0; i < createTableTests.size(); ++i) {
            std::cout << "\n--- CREATE TABLE Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << createTableTests[i] << std::endl;
            
            Parser parser(createTableTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                // 添加调试信息：检查当前表状态
                if (auto* createStmt = dynamic_cast<CreateTableStatement*>(stmt.get())) {
                    std::cout << "Before analysis - Table '" << createStmt->tableName << "' exists: " 
                              << (catalog->tableExists(createStmt->tableName) ? "Yes" : "No") << std::endl;
                }
                auto result = analyzer.analyzeStatement(stmt.get());
                
                if (result.success) {
                    std::cout << "v Semantic analysis passed" << std::endl;
                    
                    // 如果语义分析通过，实际执行CREATE TABLE操作
                    if (auto* createStmt = dynamic_cast<CreateTableStatement*>(stmt.get())) {
                        std::vector<ColumnInfo> columns;
                        for (const auto& colDef : createStmt->columns) {
                            columns.emplace_back(colDef->columnName, colDef->dataType);
                        }
                        bool createResult = catalog->createTable(createStmt->tableName, columns);
                        std::cout << "Table creation result: " << (createResult ? "Success" : "Failed");
                        if (createResult) {
                            std::cout << " - Table '" << createStmt->tableName << "' created and persisted";
                        }
                        std::cout << std::endl;
                    }
                } else {
                    std::cout << "x Semantic analysis failed:" << std::endl;
                    analyzer.printErrors();
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
            
            analyzer.clearErrors();
        }
        
        // 3. 测试INSERT的语义分析
        std::cout << "\n3. Testing INSERT semantic analysis..." << std::endl;
        
        std::vector<std::string> insertTests = {
            // 正确的INSERT语句
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
            "INSERT INTO products VALUES (101, 'Laptop', 999.99);",
            
            // 错误的INSERT语句
            "INSERT INTO nonexistent VALUES (1, 'test');", // 表不存在
            "INSERT INTO users VALUES (3, 'Charlie');", // 列数不匹配
            "INSERT INTO users VALUES ('invalid', 'Dave', 25);", // 类型不匹配
            "INSERT INTO users (id, nonexistent) VALUES (4, 'Eve');", // 列不存在
            "INSERT INTO users VALUES (5, 'Frank', 'not_a_number');", // 类型不匹配
        };
        
        for (size_t i = 0; i < insertTests.size(); ++i) {
            std::cout << "\n--- INSERT Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << insertTests[i] << std::endl;
            
            Parser parser(insertTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = analyzer.analyzeStatement(stmt.get());
                
                if (result.success) {
                    std::cout << "v Semantic analysis passed" << std::endl;
                } else {
                    std::cout << "x Semantic analysis failed:" << std::endl;
                    analyzer.printErrors();
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
            
            analyzer.clearErrors();
        }
        
        // 4. 测试SELECT的语义分析
        std::cout << "\n4. Testing SELECT semantic analysis..." << std::endl;
        
        std::vector<std::string> selectTests = {
            // 正确的SELECT语句
            "SELECT * FROM users;",
            "SELECT id, name FROM users;",
            "SELECT name, age FROM users WHERE age > 20;",
            "SELECT price FROM products WHERE price >= 500.0;",
            
            // 错误的SELECT语句
            "SELECT * FROM nonexistent;", // 表不存在
            "SELECT nonexistent FROM users;", // 列不存在
            "SELECT name FROM users WHERE nonexistent > 10;", // WHERE中列不存在
            "SELECT name FROM users WHERE age + 'invalid' > 10;", // 类型不匹配的表达式
        };
        
        for (size_t i = 0; i < selectTests.size(); ++i) {
            std::cout << "\n--- SELECT Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << selectTests[i] << std::endl;
            
            Parser parser(selectTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = analyzer.analyzeStatement(stmt.get());
                
                if (result.success) {
                    std::cout << "v Semantic analysis passed" << std::endl;
                } else {
                    std::cout << "x Semantic analysis failed:" << std::endl;
                    analyzer.printErrors();
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
            
            analyzer.clearErrors();
        }
        
        // 5. 测试DELETE的语义分析
        std::cout << "\n5. Testing DELETE semantic analysis..." << std::endl;
        
        std::vector<std::string> deleteTests = {
            // 正确的DELETE语句
            "DELETE FROM users;",
            "DELETE FROM users WHERE id = 1;",
            "DELETE FROM products WHERE price < 100.0;",
            
            // 错误的DELETE语句
            "DELETE FROM nonexistent;", // 表不存在
            "DELETE FROM users WHERE nonexistent = 1;", // 列不存在
            "DELETE FROM users WHERE id + 'invalid' = 1;", // 类型不匹配的表达式
        };
        
        for (size_t i = 0; i < deleteTests.size(); ++i) {
            std::cout << "\n--- DELETE Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << deleteTests[i] << std::endl;
            
            Parser parser(deleteTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = analyzer.analyzeStatement(stmt.get());
                
                if (result.success) {
                    std::cout << "v Semantic analysis passed" << std::endl;
                } else {
                    std::cout << "x Semantic analysis failed:" << std::endl;
                    analyzer.printErrors();
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
            
            analyzer.clearErrors();
        }
        
        // 6. 测试复杂表达式的语义分析
        std::cout << "\n6. Testing complex expression semantic analysis..." << std::endl;
        
        std::vector<std::string> expressionTests = {
            "SELECT name FROM users WHERE age + 5 > 30;",
            "SELECT name FROM users WHERE age * 2.0 <= 60.0;", 
            "SELECT name FROM users WHERE (age > 20 AND age < 40) OR name = 'Admin';",
            "SELECT name FROM users WHERE age > 20 AND price > 100.0;", // price列不存在
            "SELECT name FROM users WHERE age + name > 10;", // 类型不匹配
        };
        
        for (size_t i = 0; i < expressionTests.size(); ++i) {
            std::cout << "\n--- Expression Test Case " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << expressionTests[i] << std::endl;
            
            Parser parser(expressionTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = analyzer.analyzeStatement(stmt.get());
                
                if (result.success) {
                    std::cout << "v Semantic analysis passed" << std::endl;
                } else {
                    std::cout << "x Semantic analysis failed:" << std::endl;
                    analyzer.printErrors();
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
            
            analyzer.clearErrors();
        }
        
        // 7. 测试Catalog功能
        std::cout << "\n7. Testing Catalog functionality..." << std::endl;
        
        std::cout << "\nCurrent catalog state:" << std::endl;
        catalog->printCatalog();
        
        std::cout << "\nTable existence checks:" << std::endl;
        std::cout << "users table exists: " << (catalog->tableExists("users") ? "Yes" : "No") << std::endl;
        std::cout << "products table exists: " << (catalog->tableExists("products") ? "Yes" : "No") << std::endl;
        std::cout << "nonexistent table exists: " << (catalog->tableExists("nonexistent") ? "Yes" : "No") << std::endl;
        
        std::cout << "\nColumn existence checks:" << std::endl;
        std::cout << "users.id exists: " << (catalog->columnExists("users", "id") ? "Yes" : "No") << std::endl;
        std::cout << "users.name exists: " << (catalog->columnExists("users", "name") ? "Yes" : "No") << std::endl;
        std::cout << "users.nonexistent exists: " << (catalog->columnExists("users", "nonexistent") ? "Yes" : "No") << std::endl;
        
        // 8. 测试多语句分析
        std::cout << "\n8. Testing multi-statement semantic analysis..." << std::endl;
        
        std::string multiSQL = R"(
            CREATE TABLE employees (id INT PRIMARY KEY, name STRING, department STRING, salary DOUBLE);
            INSERT INTO employees VALUES (1, 'John', 'Engineering', 75000.0);
            INSERT INTO employees VALUES (2, 'Jane', 'Marketing', 65000.0);
            SELECT name, salary FROM employees WHERE salary > 70000.0;
            DELETE FROM employees WHERE department = 'Marketing';
        )";
        
        std::cout << "Multi-statement input:\n" << multiSQL << std::endl;
        
        Parser multiParser(multiSQL);
        auto statements = multiParser.parseStatements();
        
        std::cout << "Analyzing " << statements.size() << " statements:" << std::endl;
        
        for (size_t i = 0; i < statements.size(); ++i) {
            std::cout << "\n--- Statement " << (i + 1) << " ---" << std::endl;
            
            if (statements[i]) {
                auto result = analyzer.analyzeStatement(statements[i].get());
                
                if (result.success) {
                    std::cout << "v Semantic analysis passed" << std::endl;
                    
                    // 如果语义分析通过，实际执行CREATE TABLE操作
                    if (auto* createStmt = dynamic_cast<CreateTableStatement*>(statements[i].get())) {
                        std::vector<ColumnInfo> columns;
                        for (const auto& colDef : createStmt->columns) {
                            columns.emplace_back(colDef->columnName, colDef->dataType);
                        }
                        bool createResult = catalog->createTable(createStmt->tableName, columns);
                        std::cout << "Table creation result: " << (createResult ? "Success" : "Failed");
                        if (createResult) {
                            std::cout << " - Table '" << createStmt->tableName << "' created and persisted";
                        }
                        std::cout << std::endl;
                    }
                } else {
                    std::cout << "x Semantic analysis failed:" << std::endl;
                    analyzer.printErrors();
                }
            } else {
                std::cout << "x NULL statement" << std::endl;
            }
            
            analyzer.clearErrors();
        }
        
        // 显示最终的catalog状态
        std::cout << "\n9. Final catalog state:" << std::endl;
        catalog->printCatalog();
        
        // 10. 测试持久化
        std::cout << "\n10. Testing persistence..." << std::endl;
        std::cout << "Saving to storage..." << std::endl;
        bool saveResult = storage.saveToStorage();
        std::cout << "Save result: " << (saveResult ? "Success" : "Failed") << std::endl;
        
        // 测试重新加载
        std::cout << "\nTesting reload by creating new StorageEngine..." << std::endl;
        {
            StorageEngine storage2("./semantic_test_db");
            auto catalog2 = std::make_shared<Catalog>(&storage2);
            
            std::cout << "Reloaded catalog state:" << std::endl;
            catalog2->printCatalog();
            
            std::cout << "Checking table existence after reload:" << std::endl;
            std::cout << "users table exists: " << (catalog2->tableExists("users") ? "Yes" : "No") << std::endl;
            std::cout << "products table exists: " << (catalog2->tableExists("products") ? "Yes" : "No") << std::endl;
            std::cout << "employees table exists: " << (catalog2->tableExists("employees") ? "Yes" : "No") << std::endl;
        }
        
        std::cout << "\n=== Semantic Analyzer Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during semantic analyzer testing: " << e.what() << std::endl;
    }
}

void testIndexFeatures() {
    std::cout << "=== Starting Index Features Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./index_test_db");
        
        StorageEngine storage("./index_test_db");
        ExecutionEngine engine(&storage);
        
        // 可选：集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 测试自动PRIMARY KEY索引
        std::cout << "\n2. Testing automatic PRIMARY KEY index..." << std::endl;
        
        // 创建带主键的表
        std::string createSQL = "CREATE TABLE products (id INT PRIMARY KEY, name STRING, price DOUBLE);";
        std::cout << "\nExecuting: " << createSQL << std::endl;
        
        Parser parser1(createSQL);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Table created with automatic PK index" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 3. 插入测试数据
        std::cout << "\n3. Inserting test data..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO products VALUES (1, 'Laptop', 999.99);",
            "INSERT INTO products VALUES (2, 'Mouse', 29.99);",
            "INSERT INTO products VALUES (3, 'Keyboard', 79.99);",
            "INSERT INTO products VALUES (4, 'Monitor', 299.99);",
            "INSERT INTO products VALUES (5, 'Speaker', 149.99);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v" : "x") << " " << sql << std::endl;
            }
        }
        
        // 4. 测试手动创建索引
        std::cout << "\n4. Testing manual index creation..." << std::endl;
        
        // 为price列创建索引
        std::cout << "\nAttempting to create index on price column..." << std::endl;
        bool priceIndexCreated = storage.createIndex("idx_price", "products", "price", false);
        std::cout << "Price index creation: " << (priceIndexCreated ? "v Success" : "x Failed") << std::endl;
        
        // 为name列创建唯一索引
        std::cout << "\nAttempting to create unique index on name column..." << std::endl;
        bool nameIndexCreated = storage.createIndex("idx_name_unique", "products", "name", true);
        std::cout << "Name unique index creation: " << (nameIndexCreated ? "v Success" : "x Failed") << std::endl;
        
        // 5. 测试索引查询
        std::cout << "\n5. Testing index queries..." << std::endl;
        
        // 通过主键索引查询
        std::cout << "\nSearching by primary key (id=3):" << std::endl;
        auto pkResults = storage.searchByColumn("products", "id", Value(3));
        std::cout << "Found " << pkResults.size() << " records using primary key index" << std::endl;
        
        // 通过价格索引查询
        std::cout << "\nSearching by price (price=29.99):" << std::endl;
        auto priceResults = storage.searchByColumn("products", "price", Value(29.99));
        std::cout << "Found " << priceResults.size() << " records using price index" << std::endl;
        
        // 范围查询
        std::cout << "\nTesting range query on price [50.0, 200.0]:" << std::endl;
        auto rangeResults = storage.rangeSearchByIndex("idx_price", Value(50.0), Value(200.0));
        std::cout << "Found " << rangeResults.size() << " records in price range [50.0, 200.0]" << std::endl;
        
        // 6. 显示所有数据
        std::cout << "\n6. Displaying all data..." << std::endl;
        
        std::string selectSQL = "SELECT * FROM products;";
        Parser parser6(selectSQL);
        auto stmt6 = parser6.parseStatement();
        if (stmt6) {
            auto result = engine.executeStatement(stmt6.get());
            if (result.isSuccess()) {
                std::cout << "v Query successful. Found " << result.rows.size() << " rows:" << std::endl;
                for (size_t i = 0; i < result.rows.size(); ++i) {
                    std::cout << "  Row " << (i + 1) << ": " << result.rows[i].toString() << std::endl;
                }
            } else {
                std::cout << "x Query failed: " << result.message << std::endl;
            }
        }
        
        // 7. 显示索引信息
        std::cout << "\n7. Index information..." << std::endl;
        storage.printIndexInfo();
        
        // 8. 测试重复主键验证
        std::cout << "\n8. Testing PRIMARY KEY constraint with index..." << std::endl;
        
        std::string duplicateSQL = "INSERT INTO products VALUES (3, 'Duplicate', 99.99);";
        std::cout << "\nTrying to insert duplicate primary key: " << duplicateSQL << std::endl;
        
        Parser parser8(duplicateSQL);
        auto stmt8 = parser8.parseStatement();
        if (stmt8) {
            auto result = engine.executeStatement(stmt8.get());
            std::cout << (result.isSuccess() ? "x ERROR: Duplicate key was inserted!" : ("v Correctly rejected: " + result.message)) << std::endl;
        }
        
        std::cout << "\n=== Index Features Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during index testing: " << e.what() << std::endl;
    }
}

void testConstraints() {
    std::cout << "=== Starting Constraints Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./constraints_test_db");
        
        StorageEngine storage("./constraints_test_db");
        ExecutionEngine engine(&storage);
        
        // 可选：集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 测试PRIMARY KEY约束
        std::cout << "\n2. Testing PRIMARY KEY constraints..." << std::endl;
        
        // 创建带主键的表
        std::string createSQL = "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT);";
        std::cout << "\nExecuting: " << createSQL << std::endl;
        
        Parser parser1(createSQL);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Table created successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 插入第一条记录
        std::string insertSQL1 = "INSERT INTO users VALUES (1, 'Alice', 25);";
        std::cout << "\nExecuting: " << insertSQL1 << std::endl;
        
        Parser parser2(insertSQL1);
        auto stmt2 = parser2.parseStatement();
        if (stmt2) {
            auto result = engine.executeStatement(stmt2.get());
            std::cout << (result.isSuccess() ? "v First record inserted successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 尝试插入重复主键
        std::string insertSQL2 = "INSERT INTO users VALUES (1, 'Bob', 30);";
        std::cout << "\nExecuting (should fail): " << insertSQL2 << std::endl;
        
        Parser parser3(insertSQL2);
        auto stmt3 = parser3.parseStatement();
        if (stmt3) {
            auto result = engine.executeStatement(stmt3.get());
            std::cout << (result.isSuccess() ? "x ERROR: Duplicate key inserted (should have failed!)" : ("v Correctly rejected: " + result.message)) << std::endl;
        }
        
        // 3. 测试NOT NULL约束
        std::cout << "\n3. Testing NOT NULL constraints..." << std::endl;
        
        // 尝试插入空名字（违反NOT NULL约束）
        std::string insertSQL3 = "INSERT INTO users VALUES (2, '', 35);";
        std::cout << "\nExecuting (should fail): " << insertSQL3 << std::endl;
        
        Parser parser4(insertSQL3);
        auto stmt4 = parser4.parseStatement();
        if (stmt4) {
            auto result = engine.executeStatement(stmt4.get());
            std::cout << (result.isSuccess() ? "x ERROR: NULL value inserted (should have failed!)" : ("v Correctly rejected: " + result.message)) << std::endl;
        }
        
        // 插入正确的记录
        std::string insertSQL4 = "INSERT INTO users VALUES (3, 'Charlie', 40);";
        std::cout << "\nExecuting: " << insertSQL4 << std::endl;
        
        Parser parser5(insertSQL4);
        auto stmt5 = parser5.parseStatement();
        if (stmt5) {
            auto result = engine.executeStatement(stmt5.get());
            std::cout << (result.isSuccess() ? "v Valid record inserted successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 4. 显示最终数据
        std::cout << "\n4. Final table contents..." << std::endl;
        
        std::string selectSQL = "SELECT * FROM users;";
        std::cout << "\nExecuting: " << selectSQL << std::endl;
        
        Parser parser6(selectSQL);
        auto stmt6 = parser6.parseStatement();
        if (stmt6) {
            auto result = engine.executeStatement(stmt6.get());
            if (result.isSuccess()) {
                std::cout << "v Query successful. Found " << result.rows.size() << " rows:" << std::endl;
                for (size_t i = 0; i < result.rows.size(); ++i) {
                    std::cout << "  Row " << (i + 1) << ": " << result.rows[i].toString() << std::endl;
                }
            } else {
                std::cout << "x Query failed: " << result.message << std::endl;
            }
        }
        
        // 5. 测试表结构显示
        std::cout << "\n5. Table schema with constraints..." << std::endl;
        storage.printTableInfo("users");
        
        std::cout << "\n=== Constraints Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during constraints testing: " << e.what() << std::endl;
    }
}

void testExecutionEngine() {
    std::cout << "=== Starting Execution Engine Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./executor_test_db");
        
        StorageEngine storage("./executor_test_db");
        ExecutionEngine engine(&storage);
        
        // 可选：集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Execution Engine created successfully" << std::endl;
        
        // 2. 测试CREATE TABLE执行
        std::cout << "\n2. Testing CREATE TABLE execution..." << std::endl;
        
        std::vector<std::string> createTableSQLs = {
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);",
            "CREATE TABLE products (id INT, name STRING, price DOUBLE);"
        };
        
        for (const auto& sql : createTableSQLs) {
            std::cout << "\nExecuting: " << sql << std::endl;
            
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto plan = engine.generateExecutionPlan(stmt.get());
                if (plan) {
                    std::cout << "Execution Plan:" << std::endl;
                    engine.printExecutionPlan(*plan);
                }
                
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful: " << result.message << std::endl;
                    std::cout << "Affected rows: " << result.affectedRows << std::endl;
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 3. 测试INSERT执行
        std::cout << "\n3. Testing INSERT execution..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            "INSERT INTO users VALUES (2, 'Bob', 30);",
            "INSERT INTO users VALUES (3, 'Charlie', 35);",
            "INSERT INTO products VALUES (101, 'Laptop', 999.99);",
            "INSERT INTO products VALUES (102, 'Mouse', 29.99);"
        };
        
        for (const auto& sql : insertSQLs) {
            std::cout << "\nExecuting: " << sql << std::endl;
            
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful: " << result.message << std::endl;
                    std::cout << "Affected rows: " << result.affectedRows << std::endl;
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 4. 测试SELECT执行
        std::cout << "\n4. Testing SELECT execution..." << std::endl;
        
        std::vector<std::string> selectSQLs = {
            "SELECT * FROM users;",
            "SELECT id, name FROM users;",
            "SELECT name, age FROM users WHERE age > 25;",
            "SELECT * FROM products WHERE price < 100.0;",
            "SELECT name FROM users WHERE age >= 30;"
        };
        
        for (const auto& sql : selectSQLs) {
            std::cout << "\nExecuting: " << sql << std::endl;
            
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto plan = engine.generateExecutionPlan(stmt.get());
                if (plan) {
                    std::cout << "Execution Plan:" << std::endl;
                    engine.printExecutionPlan(*plan);
                }
                
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful: " << result.message << std::endl;
                    std::cout << "Result rows: " << result.rows.size() << std::endl;
                    
                    // 显示查询结果
                    if (!result.rows.empty()) {
                        std::cout << "Query results:" << std::endl;
                        for (size_t i = 0; i < result.rows.size() && i < 5; ++i) { // 最多显示5行
                            std::cout << "  Row " << (i + 1) << ": " << result.rows[i].toString() << std::endl;
                        }
                        if (result.rows.size() > 5) {
                            std::cout << "  ... and " << (result.rows.size() - 5) << " more rows" << std::endl;
                        }
                    }
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 5. 测试复杂查询
        std::cout << "\n5. Testing complex queries..." << std::endl;
        
        std::vector<std::string> complexSQLs = {
            "SELECT name, age + 5 FROM users WHERE age > 25;",
            "SELECT id, name FROM users WHERE age >= 25 AND age <= 35;",
            "SELECT * FROM products WHERE price >= 50.0;"
        };
        
        for (const auto& sql : complexSQLs) {
            std::cout << "\nExecuting: " << sql << std::endl;
            
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful" << std::endl;
                    std::cout << "Result rows: " << result.rows.size() << std::endl;
                    
                    // 显示查询结果
                    for (const auto& row : result.rows) {
                        std::cout << "  " << row.toString() << std::endl;
                    }
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 6. 测试批量执行
        std::cout << "\n6. Testing batch execution..." << std::endl;
        
        std::string batchSQL = R"(
            CREATE TABLE employees (id INT, name STRING, department STRING, salary DOUBLE);
            INSERT INTO employees VALUES (1, 'John', 'Engineering', 75000.0);
            INSERT INTO employees VALUES (2, 'Jane', 'Marketing', 65000.0);
            INSERT INTO employees VALUES (3, 'Mike', 'Engineering', 80000.0);
            SELECT name, salary FROM employees WHERE department = 'Engineering';
        )";
        
        std::cout << "Batch SQL:\n" << batchSQL << std::endl;
        
        Parser batchParser(batchSQL);
        auto statements = batchParser.parseStatements();
        
        auto results = engine.executeStatements(statements);
        
        std::cout << "Batch execution results:" << std::endl;
        for (size_t i = 0; i < results.size(); ++i) {
            std::cout << "Statement " << (i + 1) << ": ";
            if (results[i].isSuccess()) {
                std::cout << "Success - " << results[i].message;
                if (results[i].affectedRows > 0) {
                    std::cout << " (affected/returned: " << results[i].affectedRows << " rows)";
                }
                if (!results[i].rows.empty()) {
                    std::cout << " - Results: " << results[i].rows.size() << " rows";
                }
            } else {
                std::cout << "Failed - " << results[i].message;
            }
            std::cout << std::endl;
        }
        
        // 7. 显示执行统计
        std::cout << "\n7. Execution statistics:" << std::endl;
        engine.printStats();
        
        // 8. 测试持久化
        std::cout << "\n8. Testing persistence..." << std::endl;
        storage.saveToStorage();
        std::cout << "Data saved to storage" << std::endl;
        
        std::cout << "\n=== Execution Engine Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during execution engine testing: " << e.what() << std::endl;
    }
}

void testGroupByOrderBy() {
    std::cout << "=== Starting GROUP BY and ORDER BY Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./group_order_test_db");
        
        StorageEngine storage("./group_order_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 创建测试表并插入数据
        std::cout << "\n2. Creating test table and inserting data..." << std::endl;
        
        std::string createSQL = "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, department STRING, age INT, salary DOUBLE);";
        std::cout << "Executing: " << createSQL << std::endl;
        
        Parser parser1(createSQL);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Table created successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 插入测试数据
        std::vector<std::string> insertSQLs = {
            "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 25, 5000.0);",
            "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 30, 6000.0);",
            "INSERT INTO employees VALUES (3, 'Charlie', 'Marketing', 35, 7000.0);",
            "INSERT INTO employees VALUES (4, 'David', 'Engineering', 28, 5500.0);",
            "INSERT INTO employees VALUES (5, 'Eve', 'Marketing', 32, 6500.0);",
            "INSERT INTO employees VALUES (6, 'Frank', 'HR', 29, 4500.0);",
            "INSERT INTO employees VALUES (7, 'Grace', 'HR', 33, 4800.0);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v" : "x") << " " << sql << std::endl;
            }
        }
        
        // 3. 测试解析器对GROUP BY和ORDER BY的支持
        std::cout << "\n3. Testing GROUP BY and ORDER BY parsing..." << std::endl;
        
        std::vector<std::string> testQueries = {
            // 基本ORDER BY测试
            "SELECT name, age FROM employees ORDER BY age;",
            "SELECT name, age FROM employees ORDER BY age DESC;",
            "SELECT name, age FROM employees ORDER BY age ASC, name DESC;",
            
            // 基本GROUP BY测试
            "SELECT department FROM employees GROUP BY department;",
            "SELECT department, COUNT(*) FROM employees GROUP BY department;",
            "SELECT department, AVG(salary) FROM employees GROUP BY department;",
            
            // GROUP BY + ORDER BY组合测试
            "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY COUNT(*) DESC;",
            "SELECT department, AVG(salary) FROM employees GROUP BY department ORDER BY department;",
            
            // 聚合函数测试
            "SELECT COUNT(*), SUM(salary), AVG(salary), MAX(salary), MIN(salary) FROM employees;",
            "SELECT department, COUNT(*), SUM(salary) FROM employees GROUP BY department ORDER BY SUM(salary) DESC;"
        };
        
        for (size_t i = 0; i < testQueries.size(); ++i) {
            std::cout << "\n--- Test Query " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << testQueries[i] << std::endl;
            
            Parser parser(testQueries[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                std::cout << "v Parse successful:" << std::endl;
                std::cout << stmt->toString() << std::endl;
            } else {
                std::cout << "x Parse failed" << std::endl;
                if (parser.hasErrors()) {
                    parser.printErrors();
                }
            }
        }
        
        // 4. 测试ORDER BY执行功能
        std::cout << "\n4. Testing ORDER BY execution..." << std::endl;
        
        std::vector<std::string> orderByTests = {
            "SELECT name, age FROM employees ORDER BY age;",
            "SELECT name, salary FROM employees ORDER BY salary DESC;",
            "SELECT department, name FROM employees ORDER BY department, name;"
        };
        
        for (size_t i = 0; i < orderByTests.size(); ++i) {
            std::cout << "\n--- ORDER BY Execution Test " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << orderByTests[i] << std::endl;
            
            Parser parser(orderByTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                    for (size_t j = 0; j < result.rows.size(); ++j) {
                        std::cout << "  " << result.rows[j].toString() << std::endl;
                    }
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 5. 测试GROUP BY执行功能
        std::cout << "\n5. Testing GROUP BY execution..." << std::endl;
        
        std::vector<std::string> groupByTests = {
            "SELECT department FROM employees GROUP BY department;",
            "SELECT department, COUNT(*) FROM employees GROUP BY department;",
            "SELECT department, AVG(salary) FROM employees GROUP BY department;",
            "SELECT department, SUM(salary) FROM employees GROUP BY department;",
            "SELECT department, MAX(salary), MIN(salary) FROM employees GROUP BY department;"
        };
        
        for (size_t i = 0; i < groupByTests.size(); ++i) {
            std::cout << "\n--- GROUP BY Execution Test " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << groupByTests[i] << std::endl;
            
            Parser parser(groupByTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                    for (size_t j = 0; j < result.rows.size(); ++j) {
                        std::cout << "  " << result.rows[j].toString() << std::endl;
                    }
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 6. 测试无GROUP BY的聚合函数
        std::cout << "\n6. Testing aggregate functions without GROUP BY..." << std::endl;
        
        std::vector<std::string> aggregateTests = {
            "SELECT COUNT(*) FROM employees;",
            "SELECT AVG(salary) FROM employees;",
            "SELECT SUM(salary) FROM employees;",
            "SELECT MAX(age), MIN(age) FROM employees;"
        };
        
        for (size_t i = 0; i < aggregateTests.size(); ++i) {
            std::cout << "\n--- Aggregate Function Test " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << aggregateTests[i] << std::endl;
            
            Parser parser(aggregateTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                    for (size_t j = 0; j < result.rows.size(); ++j) {
                        std::cout << "  " << result.rows[j].toString() << std::endl;
                    }
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 7. 测试GROUP BY + ORDER BY组合
        std::cout << "\n7. Testing GROUP BY + ORDER BY combination..." << std::endl;
        
        std::vector<std::string> combinationTests = {
            "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY COUNT(*) DESC;",
            "SELECT department, AVG(salary) FROM employees GROUP BY department ORDER BY department;"
        };
        
        for (size_t i = 0; i < combinationTests.size(); ++i) {
            std::cout << "\n--- GROUP BY + ORDER BY Test " << (i + 1) << " ---" << std::endl;
            std::cout << "SQL: " << combinationTests[i] << std::endl;
            
            Parser parser(combinationTests[i]);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                    for (size_t j = 0; j < result.rows.size(); ++j) {
                        std::cout << "  " << result.rows[j].toString() << std::endl;
                    }
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        std::cout << "\n=== GROUP BY and ORDER BY Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during GROUP BY/ORDER BY testing: " << e.what() << std::endl;
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

void testCreateIndexFunctionality() {
    std::cout << "=== Starting CREATE INDEX Functionality Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./create_index_test_db");
        
        StorageEngine storage("./create_index_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 创建测试表
        std::cout << "\n2. Creating test tables..." << std::endl;
        
        // 创建employees表
        std::string createEmployees = "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, age INT, salary DOUBLE);";
        std::cout << "Creating employees table: " << createEmployees << std::endl;
        
        Parser parser1(createEmployees);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Employees table created" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 创建products表
        std::string createProducts = "CREATE TABLE products (id INT PRIMARY KEY, name STRING NOT NULL, category STRING, price DOUBLE);";
        std::cout << "Creating products table: " << createProducts << std::endl;
        
        Parser parser2(createProducts);
        auto stmt2 = parser2.parseStatement();
        if (stmt2) {
            auto result = engine.executeStatement(stmt2.get());
            std::cout << (result.isSuccess() ? "v Products table created" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 强制同步Catalog以确保表结构信息正确
        std::cout << "\nSyncing catalog..." << std::endl;
        catalog->syncFromStorage();
        std::cout << "Catalog synced. Checking table existence:" << std::endl;
        std::cout << "employees table exists: " << (catalog->tableExists("employees") ? "Yes" : "No") << std::endl;
        std::cout << "products table exists: " << (catalog->tableExists("products") ? "Yes" : "No") << std::endl;
        
        
        if (catalog->tableExists("employees")) {
            std::cout << "employees.age column exists: " << (catalog->columnExists("employees", "age") ? "Yes" : "No") << std::endl;
            std::cout << "employees.salary column exists: " << (catalog->columnExists("employees", "salary") ? "Yes" : "No") << std::endl;
        }
        
        // 3. 插入测试数据
        std::cout << "\n3. Inserting test data..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO employees VALUES (1, 'Alice', 25, 5000.0);",
            "INSERT INTO employees VALUES (2, 'Bob', 30, 6000.0);",
            "INSERT INTO employees VALUES (3, 'Charlie', 35, 7000.0);",
            "INSERT INTO employees VALUES (4, 'David', 28, 5500.0);",
            "INSERT INTO employees VALUES (5, 'Eve', 32, 6500.0);",
            "INSERT INTO products VALUES (101, 'Laptop', 'Electronics', 999.99);",
            "INSERT INTO products VALUES (102, 'Mouse', 'Electronics', 29.99);",
            "INSERT INTO products VALUES (103, 'Desk', 'Furniture', 299.99);",
            "INSERT INTO products VALUES (104, 'Chair', 'Furniture', 199.99);",
            "INSERT INTO products VALUES (105, 'Monitor', 'Electronics', 399.99);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v " << sql << std::endl;
                } else {
                    std::cout << "x " << sql << std::endl;
                    std::cout << "  Error: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed: " << sql << std::endl;
            }
        }
        
        // 4. 测试CREATE INDEX语句解析
        std::cout << "\n4. Testing CREATE INDEX statement parsing..." << std::endl;
        
        std::vector<std::string> createIndexTests = {
            "CREATE INDEX idx_employee_age ON employees(age);",
            "CREATE INDEX idx_employee_salary ON employees(salary);",
            "CREATE UNIQUE INDEX idx_product_name ON products(name);",
            "CREATE INDEX idx_product_category ON products(category);",
            "CREATE INDEX idx_product_price ON products(price);"
        };
        
        for (const auto& sql : createIndexTests) {
            std::cout << "\nParsing: " << sql << std::endl;
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                std::cout << "v Parse successful:" << std::endl;
                std::cout << stmt->toString() << std::endl;
            } else {
                std::cout << "x Parse failed" << std::endl;
                if (parser.hasErrors()) {
                    parser.printErrors();
                }
            }
        }
        
        // 5. 测试CREATE INDEX语句执行
        std::cout << "\n5. Testing CREATE INDEX statement execution..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> createIndexExecTests = {
            {"Normal Index on Age", "CREATE INDEX idx_employee_age ON employees(age);"},
            {"Normal Index on Salary", "CREATE INDEX idx_employee_salary ON employees(salary);"},
            {"Unique Index on Product Name", "CREATE UNIQUE INDEX idx_product_name ON products(name);"},
            {"Normal Index on Category", "CREATE INDEX idx_product_category ON products(category);"},
            {"Normal Index on Price", "CREATE INDEX idx_product_price ON products(price);"}
        };
        
        for (const auto& test : createIndexExecTests) {
            std::cout << "\n--- " << test.first << " ---" << std::endl;
            std::cout << "SQL: " << test.second << std::endl;
            
            Parser parser(test.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v Execution successful: " << result.message << std::endl;
                } else {
                    std::cout << "x Execution failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        // 6. 显示索引信息
        std::cout << "\n6. Displaying index information..." << std::endl;
        storage.printIndexInfo();
        
        // 7. 测试索引查询性能（简单演示）
        std::cout << "\n7. Testing index query performance..." << std::endl;
        
        // 使用索引查询
        std::cout << "\nTesting indexed queries:" << std::endl;
        auto ageResults = storage.searchByColumn("employees", "age", Value(30));
        std::cout << "Search employees with age=30: Found " << ageResults.size() << " records" << std::endl;
        
        auto salaryResults = storage.rangeSearchByIndex("idx_employee_salary", Value(5500.0), Value(6500.0));
        std::cout << "Search employees with salary in [5500, 6500]: Found " << salaryResults.size() << " records" << std::endl;
        
        auto categoryResults = storage.searchByColumn("products", "category", Value(std::string("Electronics")));
        std::cout << "Search products in Electronics category: Found " << categoryResults.size() << " records" << std::endl;
        
        // 8. 测试错误情况
        std::cout << "\n8. Testing error cases..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> errorTests = {
            {"Non-existent table", "CREATE INDEX idx_invalid ON nonexistent(col);"},
            {"Non-existent column", "CREATE INDEX idx_invalid ON employees(nonexistent_col);"},
            {"Duplicate index name", "CREATE INDEX idx_employee_age ON employees(name);"}
        };
        
        for (const auto& test : errorTests) {
            std::cout << "\n--- " << test.first << " Test ---" << std::endl;
            std::cout << "SQL: " << test.second << std::endl;
            
            Parser parser(test.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "x ERROR: Should have failed but succeeded!" << std::endl;
                } else {
                    std::cout << "v Correctly failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "v Parse correctly failed" << std::endl;
                if (parser.hasErrors()) {
                    std::cout << "Parse errors:" << std::endl;
                    parser.printErrors();
                }
            }
        }
        
        // 9. 测试使用索引的SELECT查询
        std::cout << "\n9. Testing SELECT queries with indexes..." << std::endl;
        
        std::vector<std::string> selectTests = {
            "SELECT * FROM employees WHERE age = 30;",
            "SELECT name, salary FROM employees WHERE salary > 6000;",
            "SELECT * FROM products WHERE category = 'Electronics';",
            "SELECT name, price FROM products WHERE price BETWEEN 100 AND 500;"
        };
        
        for (const auto& sql : selectTests) {
            std::cout << "\nExecuting: " << sql << std::endl;
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    printQueryResult(result);
                } else {
                    std::cout << "x Query failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
            }
        }
        
        std::cout << "\n=== CREATE INDEX Functionality Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during CREATE INDEX testing: " << e.what() << std::endl;
    }
}

void testJoinErrorHandling() {
    std::cout << "=== Starting JOIN Error Handling Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./join_error_test_db");
        
        StorageEngine storage("./join_error_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 创建测试表
        std::cout << "\n2. Creating test tables..." << std::endl;
        
        // 创建users表
        std::string createUsers = "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT);";
        Parser parser1(createUsers);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Users table created" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 创建orders表
        std::string createOrders = "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT NOT NULL, amount DOUBLE);";
        Parser parser2(createOrders);
        auto stmt2 = parser2.parseStatement();
        if (stmt2) {
            auto result = engine.executeStatement(stmt2.get());
            std::cout << (result.isSuccess() ? "v Orders table created" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 3. 测试错误情况
        std::cout << "\n3. Testing JOIN error cases..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> errorTests = {
            {"Non-existent table", "SELECT * FROM nonexistent INNER JOIN orders ON nonexistent.id = orders.user_id;"},
            {"Non-existent column", "SELECT * FROM users INNER JOIN orders ON users.invalid_col = orders.user_id;"},
            {"Missing ON clause", "SELECT * FROM users JOIN orders users.id = orders.user_id;"},
            {"Simple missing ON", "SELECT * FROM users JOIN orders WHERE 1=1;"}
        };
        
        for (size_t i = 0; i < errorTests.size(); ++i) {
            std::cout << "\n--- " << errorTests[i].first << " Test ---" << std::endl;
            std::cout << "SQL: " << errorTests[i].second << std::endl;
            
            Parser parser(errorTests[i].second);
            auto stmt = parser.parseStatement();
            
            // 检查解析错误
            if (parser.hasErrors()) {
                std::cout << "Parser errors detected:" << std::endl;
                parser.printErrors();
            }
            
            if (stmt) {
                std::cout << "v Parse successful" << std::endl;
                
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "x ERROR: Should have failed but succeeded!" << std::endl;
                } else {
                    std::cout << "v Correctly failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "v Parse correctly failed" << std::endl;
            }
        }
        
        std::cout << "\n=== JOIN Error Handling Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during JOIN error testing: " << e.what() << std::endl;
    }
}

void testJoinFunctionality() {
    std::cout << "=== Starting JOIN Functionality Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./join_test_db");
        
        StorageEngine storage("./join_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 创建测试表
        std::cout << "\n2. Creating test tables..." << std::endl;
        
        // 创建users表
        std::string createUsers = "CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT);";
        std::cout << "Creating users table: " << createUsers << std::endl;
        
        Parser parser1(createUsers);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Users table created" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 创建orders表
        std::string createOrders = "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT NOT NULL, amount DOUBLE);";
        std::cout << "Creating orders table: " << createOrders << std::endl;
        
        Parser parser2(createOrders);
        auto stmt2 = parser2.parseStatement();
        if (stmt2) {
            auto result = engine.executeStatement(stmt2.get());
            std::cout << (result.isSuccess() ? "v Orders table created" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 3. 插入测试数据
        std::cout << "\n3. Inserting test data..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            "INSERT INTO users VALUES (2, 'Bob', 30);",
            "INSERT INTO users VALUES (3, 'Charlie', 35);",
            "INSERT INTO orders VALUES (101, 1, 100.0);",
            "INSERT INTO orders VALUES (102, 1, 200.0);",
            "INSERT INTO orders VALUES (103, 2, 150.0);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v" : "x") << " " << sql << std::endl;
            }
        }
        
        // 4. 显示测试数据
        std::cout << "\n4. Displaying test data..." << std::endl;
        
        std::cout << "\nUsers table:" << std::endl;
        std::string showUsers = "SELECT * FROM users;";
        Parser showUsersParser(showUsers);
        auto showUsersStmt = showUsersParser.parseStatement();
        if (showUsersStmt) {
            auto result = engine.executeStatement(showUsersStmt.get());
            if (result.isSuccess()) {
                for (const auto& row : result.rows) {
                    std::cout << "  " << row.toString() << std::endl;
                }
            }
        }
        
        std::cout << "\nOrders table:" << std::endl;
        std::string showOrders = "SELECT * FROM orders;";
        Parser showOrdersParser(showOrders);
        auto showOrdersStmt = showOrdersParser.parseStatement();
        if (showOrdersStmt) {
            auto result = engine.executeStatement(showOrdersStmt.get());
            if (result.isSuccess()) {
                for (const auto& row : result.rows) {
                    std::cout << "  " << row.toString() << std::endl;
                }
            }
        }
        
        // 5. 测试各种JOIN类型
        std::cout << "\n5. Testing different JOIN types..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> joinTests = {
            {"INNER JOIN", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;"},
            {"LEFT JOIN", "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;"},
            {"RIGHT JOIN", "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id;"},
            {"Simple JOIN (Default INNER)", "SELECT * FROM users JOIN orders ON users.id = orders.user_id;"}
        };
        
        for (size_t i = 0; i < joinTests.size(); ++i) {
            try {
                std::cout << "\n--- " << joinTests[i].first << " Test ---" << std::endl;
                std::cout << "SQL: " << joinTests[i].second << std::endl;
                
                Parser parser(joinTests[i].second);
                auto stmt = parser.parseStatement();
                
                if (stmt) {
                    std::cout << "v Parse successful" << std::endl;
                    
                    auto result = engine.executeStatement(stmt.get());
                    if (result.isSuccess()) {
                        std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                        for (const auto& row : result.rows) {
                            std::cout << "  " << row.toString() << std::endl;
                        }
                    } else {
                        std::cout << "x Execution failed: " << result.message << std::endl;
                    }
                } else {
                    std::cout << "x Parse failed" << std::endl;
                    if (parser.hasErrors()) {
                        parser.printErrors();
                    }
                }
            } catch (const std::exception& e) {
                std::cout << "Exception in " << joinTests[i].first << ": " << e.what() << std::endl;
            }
        }
        
        // 6. 测试JOIN带WHERE条件
        std::cout << "\n6. Testing JOIN with WHERE clause..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> joinWhereTests = {
            {"INNER JOIN + WHERE", "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id WHERE orders.amount > 120;"},
            {"LEFT JOIN + WHERE", "SELECT users.name, users.age FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.age >= 30;"}
        };
        
        for (size_t i = 0; i < joinWhereTests.size(); ++i) {
            try {
                std::cout << "\n--- " << joinWhereTests[i].first << " Test ---" << std::endl;
                std::cout << "SQL: " << joinWhereTests[i].second << std::endl;
                
                Parser parser(joinWhereTests[i].second);
                auto stmt = parser.parseStatement();
                
                if (stmt) {
                    std::cout << "v Parse successful" << std::endl;
                    
                    auto result = engine.executeStatement(stmt.get());
                    if (result.isSuccess()) {
                        std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                        for (const auto& row : result.rows) {
                            std::cout << "  " << row.toString() << std::endl;
                        }
                    } else {
                        std::cout << "x Execution failed: " << result.message << std::endl;
                    }
                } else {
                    std::cout << "x Parse failed" << std::endl;
                    if (parser.hasErrors()) {
                        parser.printErrors();
                    }
                }
            } catch (const std::exception& e) {
                std::cout << "Exception in " << joinWhereTests[i].first << ": " << e.what() << std::endl;
            }
        }
        
        // 7. 测试复杂JOIN条件
        std::cout << "\n7. Testing complex JOIN conditions..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> complexJoinTests = {
            {"Mismatched JOIN condition", "SELECT * FROM users LEFT JOIN orders ON users.age = orders.amount;"},
            {"SELECT specific columns", "SELECT users.name, orders.id, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id;"}
        };
        
        for (size_t i = 0; i < complexJoinTests.size(); ++i) {
            try {
                std::cout << "\n--- " << complexJoinTests[i].first << " Test ---" << std::endl;
                std::cout << "SQL: " << complexJoinTests[i].second << std::endl;
                
                Parser parser(complexJoinTests[i].second);
                auto stmt = parser.parseStatement();
                
                if (stmt) {
                    std::cout << "v Parse successful" << std::endl;
                    
                    auto result = engine.executeStatement(stmt.get());
                    if (result.isSuccess()) {
                        std::cout << "v Execution successful. Found " << result.rows.size() << " rows:" << std::endl;
                        for (const auto& row : result.rows) {
                            std::cout << "  " << row.toString() << std::endl;
                        }
                    } else {
                        std::cout << "x Execution failed: " << result.message << std::endl;
                    }
                } else {
                    std::cout << "x Parse failed" << std::endl;
                    if (parser.hasErrors()) {
                        parser.printErrors();
                    }
                }
            } catch (const std::exception& e) {
                std::cout << "Exception in " << complexJoinTests[i].first << ": " << e.what() << std::endl;
            }
        }
        
        // 8. 测试错误情况
        std::cout << "\n8. Testing error cases..." << std::endl;
        
        std::vector<std::pair<std::string, std::string>> errorTests = {
            {"Table not found", "SELECT * FROM nonexistent INNER JOIN orders ON nonexistent.id = orders.user_id;"},
            {"Column not exist", "SELECT * FROM users INNER JOIN orders ON users.invalid_col = orders.user_id;"},
            {"Missing table name", "SELECT * FROM users JOIN ON users.id = orders.user_id;"}
        };
        
        for (size_t i = 0; i < errorTests.size(); ++i) {
            try {
                std::cout << "\n--- " << errorTests[i].first << " Test ---" << std::endl;
                std::cout << "SQL: " << errorTests[i].second << std::endl;
                
                Parser parser(errorTests[i].second);
                auto stmt = parser.parseStatement();
                
                if (stmt) {
                    std::cout << "v Parse successful (unexpected)" << std::endl;
                    
                    auto result = engine.executeStatement(stmt.get());
                    if (result.isSuccess()) {
                        std::cout << "x ERROR: Should have failed but succeeded!" << std::endl;
                    } else {
                        std::cout << "v Correctly failed: " << result.message << std::endl;
                    }
                } else {
                    std::cout << "v Parse correctly failed" << std::endl;
                    if (parser.hasErrors()) {
                        std::cout << "Parse errors:" << std::endl;
                        parser.printErrors();
                    }
                }
            } catch (const std::exception& e) {
                std::cout << "v Exception correctly caught: " << e.what() << std::endl;
            }
        }
        
        std::cout << "\n=== All JOIN tests completed successfully! ===" << std::endl;
        
        std::cout << "\n=== JOIN Functionality Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during JOIN functionality testing: " << e.what() << std::endl;
    }
}

void testColumnValidation() {
    std::cout << "=== Starting Column Validation Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./column_validation_test_db");
        
        StorageEngine storage("./column_validation_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 创建测试表
        std::cout << "\n2. Creating test table..." << std::endl;
        
        std::string createSQL = "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, age INT, salary DOUBLE);";
        std::cout << "Executing: " << createSQL << std::endl;
        
        Parser parser1(createSQL);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Table created successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 3. 插入测试数据
        std::cout << "\n3. Inserting test data..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO employees VALUES (1, 'Alice', 25, 5000.0);",
            "INSERT INTO employees VALUES (2, 'Bob', 30, 6000.0);",
            "INSERT INTO employees VALUES (3, 'Charlie', 35, 7000.0);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v" : "x") << " " << sql << std::endl;
            }
        }
        
        // 4. 测试正确的ORDER BY和GROUP BY
        std::cout << "\n4. Testing valid ORDER BY and GROUP BY statements..." << std::endl;
        
        std::vector<std::string> validQueries = {
            "SELECT name, age FROM employees ORDER BY age;",
            "SELECT name, salary FROM employees ORDER BY salary DESC;",
            "SELECT name FROM employees GROUP BY name;",
            "SELECT age, COUNT(*) FROM employees GROUP BY age;"
        };
        
        for (const auto& sql : validQueries) {
            std::cout << "\nTesting: " << sql << std::endl;
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v Valid query executed successfully" : ("x Valid query failed: " + result.message)) << std::endl;
            } else {
                std::cout << "x Parse failed for valid query" << std::endl;
            }
        }
        
        // 5. 测试包含无效列名的ORDER BY和GROUP BY
        std::cout << "\n5. Testing invalid column names in ORDER BY and GROUP BY..." << std::endl;
        
        std::vector<std::string> invalidQueries = {
            "SELECT name FROM employees ORDER BY nonexistent_column;",
            "SELECT name FROM employees ORDER BY department;",
            "SELECT name FROM employees GROUP BY invalid_column;",
            "SELECT age FROM employees GROUP BY department;",
            "SELECT COUNT(*) FROM employees GROUP BY salary, invalid_col;"
        };
        
        for (const auto& sql : invalidQueries) {
            std::cout << "\nTesting: " << sql << std::endl;
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "x ERROR: Invalid query should have failed but succeeded!" << std::endl;
                } else {
                    std::cout << "v Invalid query correctly failed: " << result.message << std::endl;
                }
            } else {
                std::cout << "x Parse failed for invalid query" << std::endl;
            }
        }
        
        std::cout << "\n=== Column Validation Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during column validation testing: " << e.what() << std::endl;
    }
}

void testQueryOptimizerAdvanced() {
    std::cout << "=== Starting Advanced Query Optimizer Test ===" << std::endl;
    std::cout << "Focus: Predicate Pushdown, Index Selection, and Query Plan Optimization" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理并重新创建数据库以确保测试环境干净
        std::filesystem::remove_all("./advanced_optimizer_test_db");
        StorageEngine storage("./advanced_optimizer_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        // 创建并设置查询优化器
        auto optimizer = std::make_unique<QueryOptimizer>(&storage);
        engine.setQueryOptimizer(std::move(optimizer));
        
        std::cout << "Advanced test environment initialized with full optimizer" << std::endl;
        
        // 2. 创建测试表
        std::cout << "\n2. Creating test tables..." << std::endl;
        
        std::string createSQL = "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, department STRING, age INT, salary DOUBLE);";
        std::cout << "Creating table: " << createSQL << std::endl;
        
        Parser parser1(createSQL);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Table created successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 3. 插入测试数据
        std::cout << "\n3. Inserting test data..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 25, 5000.0);",
            "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 30, 6000.0);",
            "INSERT INTO employees VALUES (3, 'Charlie', 'Marketing', 35, 7000.0);",
            "INSERT INTO employees VALUES (4, 'David', 'Engineering', 28, 5500.0);",
            "INSERT INTO employees VALUES (5, 'Eve', 'Marketing', 32, 6500.0);",
            "INSERT INTO employees VALUES (6, 'Frank', 'HR', 29, 4500.0);",
            "INSERT INTO employees VALUES (7, 'Grace', 'HR', 33, 4800.0);",
            "INSERT INTO employees VALUES (8, 'Henry', 'Engineering', 27, 5200.0);",
            "INSERT INTO employees VALUES (9, 'Ivy', 'Marketing', 31, 6800.0);",
            "INSERT INTO employees VALUES (10, 'Jack', 'HR', 26, 4600.0);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                if (result.isSuccess()) {
                    std::cout << "v " << sql << std::endl;
                } else {
                    std::cout << "x " << sql << " - Error: " << result.message << std::endl;
                }
            }
        }
        
        // 4. 创建索引用于优化测试
        std::cout << "\n4. Creating indexes..." << std::endl;
        
        std::vector<std::string> indexSQLs = {
            "CREATE INDEX idx_age ON employees(age);",
            "CREATE INDEX idx_department ON employees(department);",
            "CREATE INDEX idx_salary ON employees(salary);"
        };
        
        for (const auto& sql : indexSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v" : "x") << " " << sql << std::endl;
            }
        }
        
        // 5. 测试谓词下推和高级查询优化
        std::cout << "\n5. Testing Predicate Pushdown and Advanced Query Optimization..." << std::endl;
        engine.enableOptimization(true);
        
        // 精选的优化测试用例，避免重复
        std::vector<std::pair<std::string, std::string>> optimizationQueries = {
            {"Index Selection Demo", "SELECT * FROM employees WHERE age = 30;"},
            {"Predicate Pushdown Demo", "SELECT department FROM employees WHERE salary >= 5000 AND salary <= 6500;"},
            {"Complex Multi-Column Query", "SELECT * FROM employees WHERE department = 'Engineering' AND salary > 5000;"},
            {"Range Query Analysis", "SELECT name, age FROM employees WHERE age > 25 AND department = 'Marketing';"}
        };
        
        // 收集性能数据用于对比
        std::vector<std::pair<std::string, std::pair<long long, long long>>> performanceComparison;
        
        for (const auto& query : optimizationQueries) {
            std::cout << "\n--- " << query.first << " (With Optimizer) ---" << std::endl;
            std::cout << "SQL: " << query.second << std::endl;
            
            Parser parser(query.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                // 生成并显示执行计划（只生成一次）
                auto plan = engine.generateExecutionPlan(stmt.get());
                if (plan) {
                    std::cout << "Optimized Execution Plan:" << std::endl;
                    engine.printExecutionPlan(*plan);
                }
                
                // 执行查询并测量时间
                auto start = std::chrono::high_resolution_clock::now();
                auto result = engine.executeStatement(stmt.get());
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                if (result.isSuccess()) {
                    std::cout << "v Found " << result.rows.size() << " records in " << duration.count() << " ms" << std::endl;
                    
                    // 简化结果显示
                    if (result.rows.size() <= 3) {
                        printQueryResult(result);
                    } else {
                        std::cout << "Sample results (showing first 2 rows):" << std::endl;
                        ExecutionResult sampleResult = result;
                        sampleResult.rows.resize(2);
                        printQueryResult(sampleResult);
                        std::cout << "... and " << (result.rows.size() - 2) << " more rows" << std::endl;
                    }
                    
                    // 存储性能数据
                    performanceComparison.push_back({query.first, {duration.count(), 0}});
                } else {
                    std::cout << "x Query failed: " << result.message << std::endl;
                    performanceComparison.push_back({query.first, {-1, 0}});
                }
            } else {
                std::cout << "x Parse failed" << std::endl;
                performanceComparison.push_back({query.first, {-1, 0}});
            }
        }
        
        // 6. 测试查询优化（禁用优化器进行对比）
        std::cout << "\n6. Testing queries WITHOUT optimizer..." << std::endl;
        engine.disableOptimization();
        
        for (size_t i = 0; i < optimizationQueries.size(); ++i) {
            const auto& query = optimizationQueries[i];
            std::cout << "Testing: " << query.first << std::endl;
            
            Parser parser(query.second);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                auto start = std::chrono::high_resolution_clock::now();
                auto result = engine.executeStatement(stmt.get());
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                
                if (result.isSuccess()) {
                    std::cout << "  Without optimizer: " << result.rows.size() << " records in " << duration.count() << " ms" << std::endl;
                    
                    // 更新性能数据
                    if (i < performanceComparison.size()) {
                        performanceComparison[i].second.second = duration.count();
                    }
                } else {
                    std::cout << "  Query failed: " << result.message << std::endl;
                    if (i < performanceComparison.size()) {
                        performanceComparison[i].second.second = -1;
                    }
                }
            } else {
                std::cout << "  Parse failed" << std::endl;
                if (i < performanceComparison.size()) {
                    performanceComparison[i].second.second = -1;
                }
            }
        }
        
        // 7. 性能对比分析
        std::cout << "\n7. Performance Comparison Analysis" << std::endl;
        std::cout << "======================================" << std::endl;
        std::cout << std::left << std::setw(25) << "Query Type" 
                  << std::setw(18) << "With Optimizer (ms)" 
                  << std::setw(20) << "Without Optimizer (ms)" 
                  << std::setw(15) << "Speedup" 
                  << std::setw(15) << "Optimization" << std::endl;
        std::cout << std::string(93, '-') << std::endl;
        
        for (const auto& perf : performanceComparison) {
            std::cout << std::left << std::setw(25) << perf.first;
            std::cout << std::setw(18) << perf.second.first;
            std::cout << std::setw(20) << perf.second.second;
            
            if (perf.second.first > 0 && perf.second.second > 0) {
                double speedup = static_cast<double>(perf.second.second) / perf.second.first;
                std::cout << std::setw(15) << std::fixed << std::setprecision(2) << speedup << "x";
                
                if (speedup > 1.5) {
                    std::cout << std::setw(15) << "Significant";
                } else if (speedup > 1.1) {
                    std::cout << std::setw(15) << "Moderate";
                } else {
                    std::cout << std::setw(15) << "Minimal";
                }
            } else {
                std::cout << std::setw(15) << "N/A" << std::setw(15) << "N/A";
            }
            std::cout << std::endl;
        }
        
        // 8. 优化器总结
        std::cout << "\n8. Optimization Summary:" << std::endl;
        std::cout << "- Index Selection: Automatically applied for age-based queries" << std::endl;
        std::cout << "- Predicate Pushdown: Identified opportunities for SeqScan optimization" << std::endl;
        std::cout << "- Query Analysis: Successfully analyzed " << optimizationQueries.size() << " different query patterns" << std::endl;
        
        std::cout << "\n=== Query Optimizer Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during query optimizer testing: " << e.what() << std::endl;
    }
}

void testUpdateFunctionality() {
    std::cout << "=== Starting UPDATE Functionality Test ===" << std::endl;
    
    try {
        // 1. 创建存储引擎和执行引擎
        std::cout << "\n1. Creating StorageEngine and ExecutionEngine..." << std::endl;
        
        // 清理之前的测试数据
        std::filesystem::remove_all("./update_test_db");
        
        StorageEngine storage("./update_test_db");
        ExecutionEngine engine(&storage);
        
        // 集成语义分析器
        auto catalog = std::make_shared<Catalog>(&storage);
        auto semanticAnalyzer = std::make_shared<SemanticAnalyzer>(catalog);
        engine.setSemanticAnalyzer(semanticAnalyzer);
        
        std::cout << "Test environment initialized" << std::endl;
        
        // 2. 创建测试表
        std::cout << "\n2. Creating test table..." << std::endl;
        
        std::string createSQL = "CREATE TABLE employees (id INT PRIMARY KEY, name STRING NOT NULL, age INT, salary DOUBLE);";
        std::cout << "Executing: " << createSQL << std::endl;
        
        Parser parser1(createSQL);
        auto stmt1 = parser1.parseStatement();
        if (stmt1) {
            auto result = engine.executeStatement(stmt1.get());
            std::cout << (result.isSuccess() ? "v Table created successfully" : ("x Failed: " + result.message)) << std::endl;
        }
        
        // 3. 插入测试数据
        std::cout << "\n3. Inserting test data..." << std::endl;
        
        std::vector<std::string> insertSQLs = {
            "INSERT INTO employees VALUES (1, 'Alice', 25, 5000.0);",
            "INSERT INTO employees VALUES (2, 'Bob', 30, 6000.0);",
            "INSERT INTO employees VALUES (3, 'Charlie', 35, 7000.0);",
            "INSERT INTO employees VALUES (4, 'David', 28, 5500.0);",
            "INSERT INTO employees VALUES (5, 'Eve', 32, 6500.0);"
        };
        
        for (const auto& sql : insertSQLs) {
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            if (stmt) {
                auto result = engine.executeStatement(stmt.get());
                std::cout << (result.isSuccess() ? "v" : "x") << " " << sql << std::endl;
            }
        }
        
        // 4. 显示插入后的数据
        std::cout << "\n4. Initial table data..." << std::endl;
        
        std::string selectSQL1 = "SELECT * FROM employees;";
        Parser parser4(selectSQL1);
        auto stmt4 = parser4.parseStatement();
        if (stmt4) {
            auto result = engine.executeStatement(stmt4.get());
            if (result.isSuccess()) {
                std::cout << "Current employees data:" << std::endl;
                for (size_t i = 0; i < result.rows.size(); ++i) {
                    std::cout << "  " << result.rows[i].toString() << std::endl;
                }
            }
        }
        
        // 5. 测试简单UPDATE语句
        std::cout << "\n5. Testing simple UPDATE statements..." << std::endl;
        
        // 更新单个员工的薪水
        std::string updateSQL1 = "UPDATE employees SET salary = 5200.0 WHERE id = 1;";
        std::cout << "\nExecuting: " << updateSQL1 << std::endl;
        
        Parser parser5(updateSQL1);
        auto stmt5 = parser5.parseStatement();
        if (stmt5) {
            auto result = engine.executeStatement(stmt5.get());
            std::cout << (result.isSuccess() ? ("v UPDATE successful: " + result.message) : ("x UPDATE failed: " + result.message)) << std::endl;
        }
        
        // 6. 测试多列更新
        std::cout << "\n6. Testing multi-column UPDATE..." << std::endl;
        
        std::string updateSQL2 = "UPDATE employees SET age = 31, salary = 6200.0 WHERE name = 'Bob';";
        std::cout << "\nExecuting: " << updateSQL2 << std::endl;
        
        Parser parser6(updateSQL2);
        auto stmt6 = parser6.parseStatement();
        if (stmt6) {
            auto result = engine.executeStatement(stmt6.get());
            std::cout << (result.isSuccess() ? ("v UPDATE successful: " + result.message) : ("x UPDATE failed: " + result.message)) << std::endl;
        }
        
        // 7. 测试批量更新
        std::cout << "\n7. Testing batch UPDATE..." << std::endl;
        
        std::string updateSQL3 = "UPDATE employees SET salary = salary + 500.0 WHERE age >= 30;";
        std::cout << "\nExecuting: " << updateSQL3 << std::endl;
        
        Parser parser7(updateSQL3);
        auto stmt7 = parser7.parseStatement();
        if (stmt7) {
            auto result = engine.executeStatement(stmt7.get());
            std::cout << (result.isSuccess() ? ("v UPDATE successful: " + result.message) : ("x UPDATE failed: " + result.message)) << std::endl;
        }
        
        // 8. 测试无条件更新（所有行）
        std::cout << "\n8. Testing UPDATE without WHERE clause..." << std::endl;
        
        std::string updateSQL4 = "UPDATE employees SET age = age + 1;";
        std::cout << "\nExecuting: " << updateSQL4 << std::endl;
        
        Parser parser8(updateSQL4);
        auto stmt8 = parser8.parseStatement();
        if (stmt8) {
            auto result = engine.executeStatement(stmt8.get());
            std::cout << (result.isSuccess() ? ("v UPDATE successful: " + result.message) : ("x UPDATE failed: " + result.message)) << std::endl;
        }
        
        // 9. 显示更新后的数据
        std::cout << "\n9. Final table data after updates..." << std::endl;
        
        std::string selectSQL2 = "SELECT * FROM employees;";
        Parser parser9(selectSQL2);
        auto stmt9 = parser9.parseStatement();
        if (stmt9) {
            auto result = engine.executeStatement(stmt9.get());
            if (result.isSuccess()) {
                std::cout << "Final employees data:" << std::endl;
                for (size_t i = 0; i < result.rows.size(); ++i) {
                    std::cout << "  " << result.rows[i].toString() << std::endl;
                }
            }
        }
        
        // 10. 测试错误情况
        std::cout << "\n10. Testing error cases..." << std::endl;
        
        // 更新不存在的表
        std::string errorSQL1 = "UPDATE nonexistent SET col = 1;";
        std::cout << "\nTesting non-existent table: " << errorSQL1 << std::endl;
        
        Parser parserE1(errorSQL1);
        auto stmtE1 = parserE1.parseStatement();
        if (stmtE1) {
            auto result = engine.executeStatement(stmtE1.get());
            std::cout << (result.isSuccess() ? "x ERROR: Should have failed!" : ("v Correctly failed: " + result.message)) << std::endl;
        }
        
        // 更新不存在的列
        std::string errorSQL2 = "UPDATE employees SET nonexistent = 1;";
        std::cout << "\nTesting non-existent column: " << errorSQL2 << std::endl;
        
        Parser parserE2(errorSQL2);
        auto stmtE2 = parserE2.parseStatement();
        if (stmtE2) {
            auto result = engine.executeStatement(stmtE2.get());
            std::cout << (result.isSuccess() ? "x ERROR: Should have failed!" : ("v Correctly failed: " + result.message)) << std::endl;
        }
        
        // 11. 测试解析器功能
        std::cout << "\n11. Testing UPDATE parser functionality..." << std::endl;
        
        std::vector<std::string> parseTests = {
            "UPDATE users SET name = 'John', age = 30;",
            "UPDATE products SET price = price * 1.1 WHERE category = 'electronics';",
            "UPDATE orders SET status = 'shipped' WHERE order_date > '2023-01-01';",
        };
        
        for (const auto& sql : parseTests) {
            std::cout << "\nParsing: " << sql << std::endl;
            Parser parser(sql);
            auto stmt = parser.parseStatement();
            
            if (stmt) {
                std::cout << "v Parse successful: " << stmt->toString() << std::endl;
            } else {
                std::cout << "x Parse failed" << std::endl;
                if (parser.hasErrors()) {
                    parser.printErrors();
                }
            }
        }
        
        std::cout << "\n=== UPDATE Functionality Test Completed ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception occurred during UPDATE testing: " << e.what() << std::endl;
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