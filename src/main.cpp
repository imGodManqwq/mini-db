#include "storage/Table.h"
#include "storage/BufferPool.h"
#include <iostream>
#include <vector>

void testPageOperations() {
    std::cout << "\n=== 测试页式存储功能 ===" << std::endl;
    
    // 创建缓冲池，使用LRU策略
    BufferPool bufferPool(3, CachePolicy::LRU);
    
    // 启用日志
    bufferPool.enableLogging("buffer_pool.log");
    
    // 分配页面
    int page1 = bufferPool.allocatePage();
    int page2 = bufferPool.allocatePage();
    int page3 = bufferPool.allocatePage();
    
    // 获取页面并写入数据
    Page* p1 = bufferPool.getPage(page1);
    Page* p2 = bufferPool.getPage(page2);
    Page* p3 = bufferPool.getPage(page3);
    
    // 写入测试数据
    std::string data1 = "Hello Page 1";
    std::string data2 = "Hello Page 2";
    std::string data3 = "Hello Page 3";
    
    p1->writeData(0, data1.c_str(), data1.size());
    p2->writeData(0, data2.c_str(), data2.size());
    p3->writeData(0, data3.c_str(), data3.size());
    
    // 测试缓存命中
    std::cout << "\n--- 测试缓存命中 ---" << std::endl;
    Page* p1_again = bufferPool.getPage(page1);  // 应该命中缓存
    Page* p2_again = bufferPool.getPage(page2);  // 应该命中缓存
    
    // 测试缓存未命中（分配第4个页面，应该驱逐一个）
    std::cout << "\n--- 测试缓存未命中 ---" << std::endl;
    int page4 = bufferPool.allocatePage();
    Page* p4 = bufferPool.getPage(page4);
    std::string data4 = "Hello Page 4";
    p4->writeData(0, data4.c_str(), data4.size());
    
    // 再次访问被驱逐的页面
    Page* p3_miss = bufferPool.getPage(page3);  // 应该未命中，从磁盘加载
    
    // 打印统计信息
    bufferPool.printStats();
    
    // 刷新所有页面
    bufferPool.flushAll();
    
    bufferPool.disableLogging();
}

void testTableOperations() {
    std::cout << "\n=== 测试表操作功能 ===" << std::endl;
    
    // 创建表
    Table table("TestTable", 4096, 2);  // 小缓冲池用于测试驱逐
    
    // 插入多条记录
    std::vector<std::vector<std::string>> testRecords = {
        {"Alice", "Johnson", "Computer Science", "A"},
        {"Bob", "Smith", "Mathematics", "B+"},
        {"Charlie", "Brown", "Physics", "A-"},
        {"Diana", "Wilson", "Chemistry", "B"},
        {"Eve", "Davis", "Biology", "A+"},
        {"Frank", "Miller", "Engineering", "B+"},
        {"Grace", "Taylor", "Literature", "A"},
        {"Henry", "Anderson", "History", "B-"}
    };
    
    std::vector<int> recordIDs;
    
    std::cout << "\n--- 插入记录 ---" << std::endl;
    for (const auto& record : testRecords) {
        int id = table.insertRecord(record);
        recordIDs.push_back(id);
    }
    
    // 查询记录
    std::cout << "\n--- 查询记录 ---" << std::endl;
    for (int id : recordIDs) {
        table.queryRecord(id);
    }
    
    // 更新记录
    std::cout << "\n--- 更新记录 ---" << std::endl;
    std::vector<std::string> updatedRecord = {"Alice", "Johnson", "Computer Science", "A+"};
    table.updateRecord(recordIDs[0], updatedRecord);
    table.queryRecord(recordIDs[0]);
    
    // 删除记录
    std::cout << "\n--- 删除记录 ---" << std::endl;
    table.deleteRecord(recordIDs[1]);
    table.queryRecord(recordIDs[1]);  // 应该找不到
    
    // 打印表信息
    table.printTableInfo();
    
    // 刷新所有数据
    table.flushAll();
}

void testCachePolicies() {
    std::cout << "\n=== 测试不同缓存策略 ===" << std::endl;
    
    // 测试LRU策略
    std::cout << "\n--- LRU策略测试 ---" << std::endl;
    BufferPool lruPool(3, CachePolicy::LRU);
    
    int p1 = lruPool.allocatePage();
    int p2 = lruPool.allocatePage();
    int p3 = lruPool.allocatePage();
    
    // 访问顺序：p1, p2, p3, p1, p4 (应该驱逐p2)
    lruPool.getPage(p1);
    lruPool.getPage(p2);
    lruPool.getPage(p3);
    lruPool.getPage(p1);  // 再次访问p1，使其成为最近使用的
    int p4 = lruPool.allocatePage();  // 应该驱逐p2
    
    lruPool.printStats();
    
    // 测试FIFO策略
    std::cout << "\n--- FIFO策略测试 ---" << std::endl;
    BufferPool fifoPool(3, CachePolicy::FIFO);
    
    int f1 = fifoPool.allocatePage();
    int f2 = fifoPool.allocatePage();
    int f3 = fifoPool.allocatePage();
    
    // 访问顺序：f1, f2, f3, f1, f4 (应该驱逐f1)
    fifoPool.getPage(f1);
    fifoPool.getPage(f2);
    fifoPool.getPage(f3);
    fifoPool.getPage(f1);  // 再次访问f1，但FIFO不考虑访问频率
    int f4 = fifoPool.allocatePage();  // 应该驱逐f1
    
    fifoPool.printStats();
}

int main() {
    std::cout << "=== Mini-DB 存储系统测试 ===" << std::endl;
    
    try {
        // 测试页式存储功能
        // testPageOperations();
        
        // 测试表操作功能
        testTableOperations();
        
        // 测试不同缓存策略
        testCachePolicies();
        
        std::cout << "\n=== 所有测试完成 ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "错误: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
