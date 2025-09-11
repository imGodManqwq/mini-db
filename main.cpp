#include <iostream>
#include "./include/cli/REPL.h"
#include "./include/storage/Row.h"
#include "./include/storage/Table.h"
#include "./include/storage/BPlusTree.h"
#include "./include/storage/IndexManager.h"

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

int main() {
    std::cout << "MiniDB Started" << std::endl;
    std::cout << "Choose mode:" << std::endl;
    std::cout << "1. Test B+ Tree Index" << std::endl;
    std::cout << "2. Start REPL Interactive Mode" << std::endl;
    std::cout << "Please enter your choice (1-2): ";
    
    int choice;
    std::cin >> choice;
    
    if (choice == 1) {
        testBPlusTreeIndex();
    } 
    else {
        std::cin.ignore(); // 清除输入缓冲
        REPL repl;
        repl.run();
    }
    
    return 0;
}