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
    
    // 检查约束
    if (!validateConstraints(row)) {
        throw std::invalid_argument("Row validation failed: constraint violation");
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

uint32_t Table::fastInsertRow(const Row& row) {
    // 跳过所有验证和约束检查，直接插入
    uint32_t recordId = allocateRecordId();
    
    // 如果有页面管理器，使用快速页式存储
    if (pageManager_) {
        if (fastInsertRowToPage(row, recordId)) {
            return recordId;
        } else {
            throw std::runtime_error("Failed to fast insert row to page storage");
        }
    } else {
        // 回退到内存存储（兼容性）
        rows_.push_back(row);
        return recordId;
    }
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
        // 在删除迭代器之前保存pageId
        uint32_t pageId = it->second.pageId;
        recordLocations_.erase(it);
        
        // compactPage()可能改变了页面中其他记录的slot分配，需要重建该页面的映射
        rebuildPageRecordLocations(pageId);
        
        pageManager_->writePage(page);
    }
    
    return result;
}

void Table::rebuildPageRecordLocations(uint32_t pageId) {
    auto page = pageManager_->getPage(pageId);
    if (!page) {
        return;
    }
    
    // 先移除该页面的所有映射
    std::vector<uint32_t> recordIdsToRemove;
    for (const auto& pair : recordLocations_) {
        if (pair.second.pageId == pageId) {
            recordIdsToRemove.push_back(pair.first);
        }
    }
    
    for (uint32_t recordId : recordIdsToRemove) {
        recordLocations_.erase(recordId);
    }
    
    // 重新扫描页面，建立新的映射
    uint16_t slotCount = page->getSlotCount();
    
    for (uint16_t slotId = 0; slotId < slotCount; ++slotId) {
        std::string recordData = page->getRecord(slotId);
        if (!recordData.empty()) {
            // 从记录数据中提取recordId
            Row row = Row::deserialize(recordData);
            if (row.getFieldCount() > 0) {
                // 假设第一个字段是主键（recordId）
                auto firstField = row.getValue(0);
                uint32_t recordId = 0;
                if (std::holds_alternative<int>(firstField)) {
                    recordId = static_cast<uint32_t>(std::get<int>(firstField));
                } else {
                    continue;
                }
                
                // 建立新的映射
                recordLocations_[recordId] = RecordLocation(pageId, slotId);
            }
        }
    }
}

bool Table::updateRow(uint32_t recordId, const Row& newRow) {
    if (!validateRow(newRow)) {
        return false;
    }
    
    // 验证约束（排除当前正在更新的记录）
    if (!validateConstraints(newRow, recordId)) {
        return false;
    }
    
    if (!pageManager_) {
        // 内存存储的更新逻辑（简化）
        if (recordId > 0 && recordId <= rows_.size()) {
            rows_[recordId - 1] = newRow;
            return true;
        }
        return false;
    }
    
    // 页式存储的更新逻辑
    auto it = recordLocations_.find(recordId);
    if (it == recordLocations_.end()) {
        return false;
    }
    
    auto page = pageManager_->getPage(it->second.pageId);
    if (!page) {
        return false;
    }
    
    // 序列化新行数据
    std::string newRecordData = newRow.serialize();
    
    // 尝试就地更新记录
    bool result = page->updateRecord(it->second.slotId, newRecordData);
    if (result) {
        pageManager_->writePage(page);
        return true;
    } else {
        // 如果就地更新失败（可能是因为新数据太大），则使用删除后插入的策略
        // 但是要保持相同的recordId以避免索引不一致问题
        
        // 先保存旧的位置信息
        RecordLocation oldLocation = it->second;
        
        // 删除旧记录（但不从recordLocations_中移除）
        page->deleteRecord(oldLocation.slotId);
        pageManager_->writePage(page);
        
        // 尝试插入新记录到同一个页面
        std::string newRecordData = newRow.serialize();
        uint16_t newSlotId = page->insertRecordAndReturnSlot(newRecordData);
        
        if (newSlotId != UINT16_MAX) {
            // 成功插入到同一页面，更新位置映射
            recordLocations_[recordId] = RecordLocation(oldLocation.pageId, newSlotId);
            pageManager_->writePage(page);
            return true;
        } else {
            // 同一页面空间不足，尝试插入到其他页面
            if (insertRowToPage(newRow, recordId)) {
                return true;
            } else {
                // 插入失败，恢复原记录（简化处理，实际应该更严谨）
                return false;
            }
        }
    }
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

std::vector<uint32_t> Table::getAllRecordIds() const {
    std::vector<uint32_t> recordIds;
    
    if (pageManager_) {
        // 页式存储，从recordLocations_获取所有recordId
        for (const auto& pair : recordLocations_) {
            recordIds.push_back(pair.first);
        }
    } else {
        // 内存存储，recordId从1开始
        for (size_t i = 0; i < rows_.size(); ++i) {
            recordIds.push_back(static_cast<uint32_t>(i + 1));
        }
    }
    
    return recordIds;
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
        oss << col.name << "|" << static_cast<int>(col.type) 
            << "|" << (col.isNotNull ? 1 : 0) 
            << "|" << (col.isPrimaryKey ? 1 : 0) << "\n";
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
    auto columnCount = std::stoull(line);
    
    std::vector<ColumnInfo> columns;
    for (size_t i = 0; i < columnCount; ++i) {
        std::getline(iss, line);
        std::vector<std::string> parts;
        size_t pos = 0;
        size_t pipePos;
        
        // 分割字符串
        while ((pipePos = line.find('|', pos)) != std::string::npos) {
            parts.push_back(line.substr(pos, pipePos - pos));
            pos = pipePos + 1;
        }
        parts.push_back(line.substr(pos));
        
        if (parts.size() >= 2) {
            std::string colName = parts[0];
            int typeInt = std::stoi(parts[1]);
            DataType type = static_cast<DataType>(typeInt);
            
            bool isNotNull = (parts.size() > 2) ? (std::stoi(parts[2]) != 0) : false;
            bool isPrimaryKey = (parts.size() > 3) ? (std::stoi(parts[3]) != 0) : false;
            
            columns.emplace_back(colName, type, isNotNull, isPrimaryKey);
        }
    }
    
    Table table(tableName, columns);
    
    // 读取行数量
    std::getline(iss, line);
    auto rowCount = std::stoull(line);
    
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

bool Table::fastInsertRowToPage(const Row& row, uint32_t recordId) {
    if (!pageManager_) {
        return false;
    }
    
    // 序列化行数据
    std::string serializedRow = row.serialize();
    
    // 优化：缓存最后使用的页面，减少页面查找
    static uint32_t lastUsedPageId = 0;
    
    // 先尝试最后使用的页面
    if (lastUsedPageId != 0) {
        auto page = pageManager_->getPage(lastUsedPageId);
        if (page && page->hasSpace(serializedRow.size())) {
            uint16_t slotId = page->insertRecordAndReturnSlot(serializedRow);
            if (slotId != UINT16_MAX) {
                recordLocations_[recordId] = RecordLocation(lastUsedPageId, slotId);
                // 关键优化：不立即写盘，延迟到批量操作结束
                return true;
            }
        }
    }
    
    // 尝试在现有页面中插入
    for (uint32_t pageId : dataPageIds_) {
        auto page = pageManager_->getPage(pageId);
        if (page && page->hasSpace(serializedRow.size())) {
            uint16_t slotId = page->insertRecordAndReturnSlot(serializedRow);
            if (slotId != UINT16_MAX) {
                recordLocations_[recordId] = RecordLocation(pageId, slotId);
                lastUsedPageId = pageId; // 缓存这个页面
                // 关键优化：不立即写盘
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
        lastUsedPageId = newPageId; // 缓存新页面
        // 关键优化：不立即写盘
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

bool Table::validateConstraints(const Row& row) const {
    // 检查列数是否匹配
    if (row.getFieldCount() != columns_.size()) {
        return false;
    }
    
    // 检查NOT NULL约束
    if (!checkNotNullConstraints(row)) {
        return false;
    }
    
    // 检查PRIMARY KEY约束
    if (!checkPrimaryKeyConstraint(row)) {
        return false;
    }
    
    return true;
}

bool Table::validateConstraints(const Row& row, uint32_t excludeRecordId) const {
    // 检查列数是否匹配
    if (row.getFieldCount() != columns_.size()) {
        return false;
    }
    
    // 检查NOT NULL约束
    if (!checkNotNullConstraints(row)) {
        return false;
    }
    
    // 检查PRIMARY KEY约束（排除指定记录）
    if (!checkPrimaryKeyConstraint(row, excludeRecordId)) {
        return false;
    }
    
    return true;
}

bool Table::checkNotNullConstraints(const Row& row) const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].isNotNull) {
            const Value& value = row.getValue(i);
            
            // 检查是否为"空值"（这里简单检查字符串是否为空）
            if (std::holds_alternative<std::string>(value)) {
                const std::string& strValue = std::get<std::string>(value);
                if (strValue.empty()) {
                    std::cerr << "NOT NULL constraint violation: Column '" 
                              << columns_[i].name << "' cannot be empty" << std::endl;
                    return false;
                }
            }
            // 对于INT和DOUBLE类型，我们认为它们总是有值的
        }
    }
    return true;
}

bool Table::checkPrimaryKeyConstraint(const Row& row) const {
    int pkIndex = getPrimaryKeyColumnIndex();
    if (pkIndex == -1) {
        return true; // 没有主键列，不需要检查
    }
    
    const Value& pkValue = row.getValue(pkIndex);
    
    // 检查主键值是否与现有记录重复
    // 首先确保数据已加载
    const_cast<Table*>(this)->loadRowsFromPages();
    
    for (const auto& existingRow : rows_) {
        const Value& existingPkValue = existingRow.getValue(pkIndex);
        
        // 比较主键值
        if (std::holds_alternative<int>(pkValue) && std::holds_alternative<int>(existingPkValue)) {
            if (std::get<int>(pkValue) == std::get<int>(existingPkValue)) {
                std::cerr << "PRIMARY KEY constraint violation: Duplicate key value " 
                          << std::get<int>(pkValue) << " in column '" 
                          << columns_[pkIndex].name << "'" << std::endl;
                return false;
            }
        } else if (std::holds_alternative<std::string>(pkValue) && std::holds_alternative<std::string>(existingPkValue)) {
            if (std::get<std::string>(pkValue) == std::get<std::string>(existingPkValue)) {
                std::cerr << "PRIMARY KEY constraint violation: Duplicate key value '" 
                          << std::get<std::string>(pkValue) << "' in column '" 
                          << columns_[pkIndex].name << "'" << std::endl;
                return false;
            }
        } else if (std::holds_alternative<double>(pkValue) && std::holds_alternative<double>(existingPkValue)) {
            if (std::get<double>(pkValue) == std::get<double>(existingPkValue)) {
                std::cerr << "PRIMARY KEY constraint violation: Duplicate key value " 
                          << std::get<double>(pkValue) << " in column '" 
                          << columns_[pkIndex].name << "'" << std::endl;
                return false;
            }
        }
    }
    
    return true;
}

bool Table::checkPrimaryKeyConstraint(const Row& row, uint32_t excludeRecordId) const {
    int pkIndex = getPrimaryKeyColumnIndex();
    if (pkIndex == -1) {
        return true; // 没有主键列，不需要检查
    }
    
    const Value& pkValue = row.getValue(pkIndex);
    
    // 检查主键值是否与现有记录重复（排除指定记录）
    if (pageManager_) {
        // 使用页式存储时，遍历recordLocations_来检查
        for (const auto& pair : recordLocations_) {
            uint32_t existingRecordId = pair.first;
            
            // 跳过我们要排除的记录
            if (existingRecordId == excludeRecordId) {
                continue;
            }
            
            Row existingRow = getRow(existingRecordId);
            if (existingRow.getFieldCount() == 0) {
                continue; // 跳过无效记录
            }
            
            const Value& existingPkValue = existingRow.getValue(pkIndex);
            
            // 比较主键值
            if (std::holds_alternative<int>(pkValue) && std::holds_alternative<int>(existingPkValue)) {
                if (std::get<int>(pkValue) == std::get<int>(existingPkValue)) {
                    std::cerr << "PRIMARY KEY constraint violation: Duplicate key value " 
                              << std::get<int>(pkValue) << " in column '" 
                              << columns_[pkIndex].name << "'" << std::endl;
                    return false;
                }
            } else if (std::holds_alternative<std::string>(pkValue) && std::holds_alternative<std::string>(existingPkValue)) {
                if (std::get<std::string>(pkValue) == std::get<std::string>(existingPkValue)) {
                    std::cerr << "PRIMARY KEY constraint violation: Duplicate key value '" 
                              << std::get<std::string>(pkValue) << "' in column '" 
                              << columns_[pkIndex].name << "'" << std::endl;
                    return false;
                }
            } else if (std::holds_alternative<double>(pkValue) && std::holds_alternative<double>(existingPkValue)) {
                if (std::get<double>(pkValue) == std::get<double>(existingPkValue)) {
                    std::cerr << "PRIMARY KEY constraint violation: Duplicate key value " 
                              << std::get<double>(pkValue) << " in column '" 
                              << columns_[pkIndex].name << "'" << std::endl;
                    return false;
                }
            }
        }
    } else {
        // 内存存储的检查逻辑（简化）
        for (size_t i = 0; i < rows_.size(); ++i) {
            // 跳过我们要排除的记录（内存存储中recordId从1开始）
            if (static_cast<uint32_t>(i + 1) == excludeRecordId) {
                continue;
            }
            
            const Row& existingRow = rows_[i];
            const Value& existingPkValue = existingRow.getValue(pkIndex);
            
            // 比较主键值
            if (std::holds_alternative<int>(pkValue) && std::holds_alternative<int>(existingPkValue)) {
                if (std::get<int>(pkValue) == std::get<int>(existingPkValue)) {
                    std::cerr << "PRIMARY KEY constraint violation: Duplicate key value " 
                              << std::get<int>(pkValue) << " in column '" 
                              << columns_[pkIndex].name << "'" << std::endl;
                    return false;
                }
            } else if (std::holds_alternative<std::string>(pkValue) && std::holds_alternative<std::string>(existingPkValue)) {
                if (std::get<std::string>(pkValue) == std::get<std::string>(existingPkValue)) {
                    std::cerr << "PRIMARY KEY constraint violation: Duplicate key value '" 
                              << std::get<std::string>(pkValue) << "' in column '" 
                              << columns_[pkIndex].name << "'" << std::endl;
                    return false;
                }
            } else if (std::holds_alternative<double>(pkValue) && std::holds_alternative<double>(existingPkValue)) {
                if (std::get<double>(pkValue) == std::get<double>(existingPkValue)) {
                    std::cerr << "PRIMARY KEY constraint violation: Duplicate key value " 
                              << std::get<double>(pkValue) << " in column '" 
                              << columns_[pkIndex].name << "'" << std::endl;
                    return false;
                }
            }
        }
    }
    
    return true;
}

bool Table::hasPrimaryKeyColumn() const {
    return getPrimaryKeyColumnIndex() != -1;
}

int Table::getPrimaryKeyColumnIndex() const {
    for (size_t i = 0; i < columns_.size(); ++i) {
        if (columns_[i].isPrimaryKey) {
            return static_cast<int>(i);
        }
    }
    return -1;
}