#pragma once
#include "Row.h"
#include "RowIterator.h"
#include <vector>
#include <string>
#include <unordered_map>

// 列定义结构
struct ColumnInfo {
    std::string name;
    DataType type;
    bool isNotNull;
    bool isPrimaryKey;
    
    ColumnInfo(const std::string& n, DataType t, bool notNull = false, bool primaryKey = false) 
        : name(n), type(t), isNotNull(notNull), isPrimaryKey(primaryKey) {}
};

// 前向声明
class PageManager;

class Table {
public:
    Table(const std::string& tableName);
    Table(const std::string& tableName, const std::vector<ColumnInfo>& columns);
    Table(const std::string& tableName, const std::vector<ColumnInfo>& columns, PageManager* pageManager);
    
    // 表结构操作
    void addColumn(const std::string& name, DataType type);
    const std::vector<ColumnInfo>& getColumns() const;
    size_t getColumnCount() const;
    int getColumnIndex(const std::string& columnName) const;
    
    // 数据操作
    uint32_t insertRow(const Row& row);  // 返回记录ID
    uint32_t insertRow(const std::vector<Value>& values);
    // 超快速插入（跳过约束检查和立即写盘）
    uint32_t fastInsertRow(const Row& row);
    bool deleteRow(uint32_t recordId);
    bool updateRow(uint32_t recordId, const Row& newRow);
    
    // 查询操作
    RowIterator begin() const;
    RowIterator end() const;
    size_t getRowCount() const;
    Row getRow(uint32_t recordId) const;  // 根据记录ID获取行
    
    // 页面管理
    void setPageManager(PageManager* pageManager);
    
    // 表信息
    const std::string& getTableName() const;
    std::vector<uint32_t> getAllRecordIds() const;
    
    // 数据验证
    bool validateRow(const Row& row) const;
    bool validateConstraints(const Row& row) const;
    bool validateConstraints(const Row& row, uint32_t excludeRecordId) const;
    bool checkNotNullConstraints(const Row& row) const;
    bool checkPrimaryKeyConstraint(const Row& row) const;
    bool checkPrimaryKeyConstraint(const Row& row, uint32_t excludeRecordId) const;
    bool hasPrimaryKeyColumn() const;
    int getPrimaryKeyColumnIndex() const;
    
    // 序列化（用于持久化）
    std::string serialize() const;
    static Table deserialize(const std::string& data);
    
    // 打印表结构和数据
    void printSchema() const;
    void printData() const;
    
private:
    std::string tableName_;
    std::vector<ColumnInfo> columns_;
    std::vector<Row> rows_;  // 临时保留，用于兼容性
    std::unordered_map<std::string, size_t> columnNameToIndex_;
    
    // 页面管理
    PageManager* pageManager_;
    std::vector<uint32_t> dataPageIds_;  // 表的数据页ID列表
    uint32_t nextRecordId_;
    
    // 记录ID到页面位置的映射
    struct RecordLocation {
        uint32_t pageId;
        uint16_t slotId;
        RecordLocation(uint32_t p = 0, uint16_t s = 0) : pageId(p), slotId(s) {}
    };
    std::unordered_map<uint32_t, RecordLocation> recordLocations_;
    
    void buildColumnIndex();
    uint32_t allocateRecordId();
    bool insertRowToPage(const Row& row, uint32_t recordId);
    // 快速插入到页面（跳过约束检查和立即写盘）
    bool fastInsertRowToPage(const Row& row, uint32_t recordId);
    void loadRowsFromPages();  // 从页面加载数据到内存数组（用于迭代器兼容性）
};
