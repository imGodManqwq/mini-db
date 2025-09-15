#include "../../include/storage/Table.h"
#include "../../include/storage/PageManager.h"
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <climits>

Table::Table(const std::string& tableName) 
    : tableName_(tableName), pageManager_(nullptr), nextRecordId_(1) {}

Table::Table(const std::string& tableName, const std::vector<ColumnInfo>& columns) 
    : tableName_(tableName), columns_(columns), pageManager_(nullptr), nextRecordId_(1) {
    buildColumnIndex();
}

Table::Table(const std::string& tableName, const std::vector<ColumnInfo>& columns, PageManager* pageManager)
    : tableName_(tableName), columns_(columns), pageManager_(pageManager), nextRecordId_(1) {
    buildColumnIndex();
}

void Table::addColumn(const std::string& name, DataType type) {
    columns_.emplace_back(name, type);
    buildColumnIndex();
}

const std::vector<ColumnInfo>& Table::getColumns() const {
    return columns_;
}

size_t Table::getColumnCount() const {
    return columns_.size();
}

int Table::getColumnIndex(const std::string& columnName) const {
    auto it = columnNameToIndex_.find(columnName);
    return (it != columnNameToIndex_.end()) ? static_cast<int>(it->second) : -1;
}

uint32_t Table::insertRow(const Row& row) {
    if (!validateRow(row)) {
        throw std::invalid_argument("Row validation failed: column count mismatch");
    }
    
    uint32_t recordId = allocateRecordId();
    
    // 如果有页面管理器，使用页式存储
    if (pageManager_) {
        if (insertRowToPage(row, recordId)) {
            return recordId;
        } else {
            throw std::runtime_error("Failed to insert row to page storage");
        }
    } else {
        // 回退到内存存储（兼容性）
        rows_.push_back(row);
        return recordId;
    }
}

uint32_t Table::insertRow(const std::vector<Value>& values) {
    Row row(values);
    return insertRow(row);
}

bool Table::deleteRow(uint32_t recordId) {
    if (!pageManager_) {
        // 内存存储的删除逻辑（简化）
        return false;
    }
    
    auto it = recordLocations_.find(recordId);
    if (it == recordLocations_.end()) {
        return false;
    }
    
    auto page = pageManager_->getPage(it->second.pageId);
    if (!page) {
        return false;
    }
    
    bool result = page->deleteRecord(it->second.slotId);
    if (result) {
        recordLocations_.erase(it);
        pageManager_->writePage(page);
    }
    
    return result;
}

bool Table::updateRow(uint32_t recordId, const Row& newRow) {
    if (!validateRow(newRow)) {
        return false;
    }
    
    // 简化实现：先删除再插入
    if (deleteRow(recordId)) {
        try {
            uint32_t newRecordId = insertRow(newRow);
            // 更新记录ID映射（如果需要保持相同ID）
            return true;
        } catch (...) {
            return false;
        }
    }
    
    return false;
}

RowIterator Table::begin() const {
    if (pageManager_) {
        // 使用页式存储时，需要重新构建内存数组用于迭代
        // 这是一个临时解决方案，实际应该实现页面迭代器
        const_cast<Table*>(this)->loadRowsFromPages();
    }
    return RowIterator(&rows_, 0);
}

RowIterator Table::end() const {
    if (pageManager_) {
        const_cast<Table*>(this)->loadRowsFromPages();
    }
    return RowIterator(&rows_, rows_.size());
}

size_t Table::getRowCount() const {
    if (pageManager_) {
        return recordLocations_.size();
    } else {
        return rows_.size();
    }
}

Row Table::getRow(uint32_t recordId) const {
    if (!pageManager_) {
        // 内存存储的简单实现
        if (recordId > 0 && recordId <= rows_.size()) {
            return rows_[recordId - 1];
        }
        return Row();
    }
    
    auto it = recordLocations_.find(recordId);
    if (it == recordLocations_.end()) {
        return Row();
    }
    
    auto page = pageManager_->getPage(it->second.pageId);
    if (!page) {
        return Row();
    }
    
    std::string recordData = page->getRecord(it->second.slotId);
    if (recordData.empty()) {
        return Row();
    }
    
    return Row::deserialize(recordData);
}

void Table::setPageManager(PageManager* pageManager) {
    pageManager_ = pageManager;
}

const std::string& Table::getTableName() const {
    return tableName_;
}

bool Table::validateRow(const Row& row) const {
    return row.getFieldCount() == columns_.size();
}

std::string Table::serialize() const {
    std::ostringstream oss;
    
    // 序列化表名
    oss << tableName_ << "\n";
    
    // 序列化列信息
    oss << columns_.size() << "\n";
    for (const auto& col : columns_) {
        oss << col.name << "|" << static_cast<int>(col.type) << "\n";
    }
    
    // 序列化行数据
    oss << rows_.size() << "\n";
    for (const auto& row : rows_) {
        oss << row.serialize() << "\n";
    }
    
    return oss.str();
}

Table Table::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string line;
    
    // 读取表名
    std::getline(iss, line);
    std::string tableName = line;
    
    // 读取列数量
    std::getline(iss, line);
    size_t columnCount = std::stoull(line);
    
    std::vector<ColumnInfo> columns;
    for (size_t i = 0; i < columnCount; ++i) {
        std::getline(iss, line);
        size_t pipePos = line.find('|');
        if (pipePos != std::string::npos) {
            std::string colName = line.substr(0, pipePos);
            int typeInt = std::stoi(line.substr(pipePos + 1));
            DataType type = static_cast<DataType>(typeInt);
            columns.emplace_back(colName, type);
        }
    }
    
    Table table(tableName, columns);
    
    // 读取行数量
    std::getline(iss, line);
    size_t rowCount = std::stoull(line);
    
    for (size_t i = 0; i < rowCount; ++i) {
        std::getline(iss, line);
        Row row = Row::deserialize(line);
        table.insertRow(row);
    }
    
    return table;
}

void Table::printSchema() const {
    std::cout << "Table: " << tableName_ << std::endl;
    std::cout << "Columns:" << std::endl;
    for (size_t i = 0; i < columns_.size(); ++i) {
        std::cout << "  " << i << ": " << columns_[i].name << " (";
        switch (columns_[i].type) {
            case DataType::INT: std::cout << "INT"; break;
            case DataType::STRING: std::cout << "STRING"; break;
            case DataType::DOUBLE: std::cout << "DOUBLE"; break;
        }
        std::cout << ")" << std::endl;
    }
}

void Table::printData() const {
    std::cout << "Data in table " << tableName_ << ":" << std::endl;
    
    if (pageManager_) {
        // 使用页式存储时，从页面中读取数据
        for (const auto& pair : recordLocations_) {
            uint32_t recordId = pair.first;
            Row row = getRow(recordId);
            if (row.getFieldCount() > 0) {
                std::cout << "  [" << recordId << "] " << row.toString() << std::endl;
            }
        }
        std::cout << "Total rows: " << recordLocations_.size() << std::endl;
    } else {
        // 内存存储
        for (const auto& row : rows_) {
            std::cout << "  " << row.toString() << std::endl;
        }
        std::cout << "Total rows: " << rows_.size() << std::endl;
    }
}

void Table::buildColumnIndex() {
    columnNameToIndex_.clear();
    for (size_t i = 0; i < columns_.size(); ++i) {
        columnNameToIndex_[columns_[i].name] = i;
    }
}

uint32_t Table::allocateRecordId() {
    return nextRecordId_++;
}

bool Table::insertRowToPage(const Row& row, uint32_t recordId) {
    if (!pageManager_) {
        return false;
    }
    
    // 序列化行数据
    std::string serializedRow = row.serialize();
    
    // 尝试在现有页面中插入
    for (uint32_t pageId : dataPageIds_) {
        auto page = pageManager_->getPage(pageId);
        if (page && page->hasSpace(serializedRow.size())) {
            uint16_t slotId = page->insertRecordAndReturnSlot(serializedRow);
            if (slotId != UINT16_MAX) {
                // 记录插入成功，保存位置信息
                recordLocations_[recordId] = RecordLocation(pageId, slotId);
                pageManager_->writePage(page);
                return true;
            }
        }
    }
    
    // 现有页面都没有足够空间，分配新页面
    uint32_t newPageId = pageManager_->allocatePage(PageType::DATA_PAGE);
    if (newPageId == 0) {
        return false;
    }
    
    auto newPage = pageManager_->getPage(newPageId);
    if (!newPage) {
        pageManager_->deallocatePage(newPageId);
        return false;
    }
    
    uint16_t slotId = newPage->insertRecordAndReturnSlot(serializedRow);
    if (slotId != UINT16_MAX) {
        dataPageIds_.push_back(newPageId);
        recordLocations_[recordId] = RecordLocation(newPageId, slotId);
        pageManager_->writePage(newPage);
        return true;
    }
    
    pageManager_->deallocatePage(newPageId);
    return false;
}

void Table::loadRowsFromPages() {
    if (!pageManager_) {
        return;
    }
    
    // 清空内存数组
    rows_.clear();
    
    // 从页面中加载所有数据
    for (const auto& pair : recordLocations_) {
        uint32_t recordId = pair.first;
        Row row = getRow(recordId);
        if (row.getFieldCount() > 0) {
            rows_.push_back(row);
        }
    }
}