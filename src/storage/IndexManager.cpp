#include "../../include/storage/IndexManager.h"
#include <iostream>
#include <algorithm>

bool IndexManager::createIndex(const std::string& indexName, const std::string& tableName,
                              const std::string& columnName, IndexType type, bool isUnique) {
    // 验证索引名称
    if (!validateIndexName(indexName)) {
        std::cerr << "Invalid index name: " << indexName << std::endl;
        return false;
    }
    
    // 检查索引是否已存在
    if (indexInfos_.find(indexName) != indexInfos_.end()) {
        std::cerr << "Index already exists: " << indexName << std::endl;
        return false;
    }
    
    // 检查表是否存在
    auto tableIt = tables_.find(tableName);
    if (tableIt == tables_.end()) {
        std::cerr << "Table not found: " << tableName << std::endl;
        return false;
    }
    
    auto table = tableIt->second;
    
    // 检查列是否存在
    int columnIndex = table->getColumnIndex(columnName);
    if (columnIndex < 0) {
        std::cerr << "Column not found: " << columnName << " in table " << tableName << std::endl;
        return false;
    }
    
    // 创建索引信息
    auto indexInfo = std::make_unique<IndexInfo>(indexName, tableName, columnName, type, isUnique);
    
    // 创建B+树索引
    if (type == IndexType::BTREE) {
        auto btree = std::make_unique<BPlusTree>();
        
        // 为现有数据建立索引
        uint32_t recordId = 0;
        for (auto it = table->begin(); it != table->end(); ++it, ++recordId) {
            Value columnValue = it->getValue(columnIndex);
            
            // 检查唯一性约束
            if (isUnique) {
                auto existingRecords = btree->search(columnValue);
                if (!existingRecords.empty()) {
                    std::cerr << "Unique constraint violation for value in column " 
                              << columnName << std::endl;
                    return false;
                }
            }
            
            btree->insert(columnValue, recordId);
        }
        
        indexes_[indexName] = std::move(btree);
        indexInfos_[indexName] = std::move(indexInfo);
        
        std::cout << "Created B+ tree index: " << indexName 
                  << " on " << tableName << "." << columnName << std::endl;
        return true;
    }
    
    std::cerr << "Unsupported index type" << std::endl;
    return false;
}

bool IndexManager::dropIndex(const std::string& indexName) {
    auto indexIt = indexInfos_.find(indexName);
    if (indexIt == indexInfos_.end()) {
        std::cerr << "Index not found: " << indexName << std::endl;
        return false;
    }
    
    indexes_.erase(indexName);
    indexInfos_.erase(indexName);
    
    std::cout << "Dropped index: " << indexName << std::endl;
    return true;
}

bool IndexManager::insertRecord(const std::string& tableName, const Row& row, uint32_t recordId) {
    // 找到该表的所有索引
    for (const auto& pair : indexInfos_) {
        const auto& indexInfo = pair.second;
        if (indexInfo->tableName != tableName) continue;
        
        const std::string& indexName = pair.first;
        auto indexIt = indexes_.find(indexName);
        if (indexIt == indexes_.end()) continue;
        
        // 提取列值
        Value columnValue = extractColumnValue(row, tableName, indexInfo->columnName);
        
        // 检查唯一性约束
        if (indexInfo->isUnique) {
            auto existingRecords = indexIt->second->search(columnValue);
            if (!existingRecords.empty()) {
                std::cerr << "Unique constraint violation for index " << indexName << std::endl;
                return false;
            }
        }
        
        // 插入到索引
        if (!indexIt->second->insert(columnValue, recordId)) {
            std::cerr << "Failed to insert into index: " << indexName << std::endl;
            return false;
        }
    }
    
    return true;
}

bool IndexManager::deleteRecord(const std::string& tableName, const Row& row, uint32_t recordId) {
    // 从该表的所有索引中删除记录
    for (const auto& pair : indexInfos_) {
        const auto& indexInfo = pair.second;
        if (indexInfo->tableName != tableName) continue;
        
        const std::string& indexName = pair.first;
        auto indexIt = indexes_.find(indexName);
        if (indexIt == indexes_.end()) continue;
        
        // 提取列值
        Value columnValue = extractColumnValue(row, tableName, indexInfo->columnName);
        
        // 从索引中删除
        if (!indexIt->second->remove(columnValue, recordId)) {
            std::cerr << "Failed to remove from index: " << indexName << std::endl;
            return false;
        }
    }
    
    return true;
}

bool IndexManager::updateRecord(const std::string& tableName, const Row& oldRow, 
                               const Row& newRow, uint32_t recordId) {
    // 先删除旧记录，再插入新记录
    if (!deleteRecord(tableName, oldRow, recordId)) {
        return false;
    }
    
    if (!insertRecord(tableName, newRow, recordId)) {
        // 如果插入失败，尝试回滚
        insertRecord(tableName, oldRow, recordId);
        return false;
    }
    
    return true;
}

std::vector<uint32_t> IndexManager::searchByIndex(const std::string& indexName, const Value& key) const {
    auto indexIt = indexes_.find(indexName);
    if (indexIt == indexes_.end()) {
        std::cerr << "Index not found: " << indexName << std::endl;
        return {};
    }
    
    return indexIt->second->search(key);
}

std::vector<uint32_t> IndexManager::rangeSearchByIndex(const std::string& indexName, 
                                                      const Value& startKey, const Value& endKey) const {
    auto indexIt = indexes_.find(indexName);
    if (indexIt == indexes_.end()) {
        std::cerr << "Index not found: " << indexName << std::endl;
        return {};
    }
    
    return indexIt->second->rangeSearch(startKey, endKey);
}

bool IndexManager::hasIndex(const std::string& tableName, const std::string& columnName) const {
    for (const auto& pair : indexInfos_) {
        const auto& indexInfo = pair.second;
        if (indexInfo->tableName == tableName && indexInfo->columnName == columnName) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> IndexManager::getIndexesForTable(const std::string& tableName) const {
    std::vector<std::string> result;
    for (const auto& pair : indexInfos_) {
        if (pair.second->tableName == tableName) {
            result.push_back(pair.first);
        }
    }
    return result;
}

const IndexInfo* IndexManager::getIndexInfo(const std::string& indexName) const {
    auto it = indexInfos_.find(indexName);
    return (it != indexInfos_.end()) ? it->second.get() : nullptr;
}

void IndexManager::registerTable(std::shared_ptr<Table> table) {
    if (table) {
        tables_[table->getTableName()] = table;
        std::cout << "Registered table for indexing: " << table->getTableName() << std::endl;
    }
}

void IndexManager::unregisterTable(const std::string& tableName) {
    // 删除该表的所有索引
    std::vector<std::string> indexesToDrop;
    for (const auto& pair : indexInfos_) {
        if (pair.second->tableName == tableName) {
            indexesToDrop.push_back(pair.first);
        }
    }
    
    for (const std::string& indexName : indexesToDrop) {
        dropIndex(indexName);
    }
    
    tables_.erase(tableName);
    std::cout << "Unregistered table: " << tableName << std::endl;
}

void IndexManager::printIndexStats() const {
    std::cout << "Index Manager Statistics:" << std::endl;
    std::cout << "  Total indexes: " << indexes_.size() << std::endl;
    std::cout << "  Registered tables: " << tables_.size() << std::endl;
    
    std::cout << "  Indexes:" << std::endl;
    for (const auto& pair : indexInfos_) {
        const auto& indexInfo = pair.second;
        auto indexIt = indexes_.find(pair.first);
        
        std::cout << "    - " << pair.first 
                  << " (" << indexInfo->tableName << "." << indexInfo->columnName << ")";
        
        if (indexIt != indexes_.end()) {
            std::cout << " [Height: " << indexIt->second->getHeight() 
                      << ", Nodes: " << indexIt->second->getNodeCount() << "]";
        }
        
        if (indexInfo->isUnique) {
            std::cout << " [UNIQUE]";
        }
        
        std::cout << std::endl;
    }
}

void IndexManager::printIndexInfo(const std::string& indexName) const {
    auto infoIt = indexInfos_.find(indexName);
    if (infoIt == indexInfos_.end()) {
        std::cout << "Index not found: " << indexName << std::endl;
        return;
    }
    
    const auto& indexInfo = infoIt->second;
    std::cout << "Index Information:" << std::endl;
    std::cout << "  Name: " << indexInfo->indexName << std::endl;
    std::cout << "  Table: " << indexInfo->tableName << std::endl;
    std::cout << "  Column: " << indexInfo->columnName << std::endl;
    std::cout << "  Type: " << (indexInfo->indexType == IndexType::BTREE ? "B+ Tree" : "Hash") << std::endl;
    std::cout << "  Unique: " << (indexInfo->isUnique ? "Yes" : "No") << std::endl;
    
    auto indexIt = indexes_.find(indexName);
    if (indexIt != indexes_.end()) {
        std::cout << "  Height: " << indexIt->second->getHeight() << std::endl;
        std::cout << "  Node Count: " << indexIt->second->getNodeCount() << std::endl;
        std::cout << "  Empty: " << (indexIt->second->isEmpty() ? "Yes" : "No") << std::endl;
        
        std::cout << "\nB+ Tree Structure:" << std::endl;
        indexIt->second->printTree();
    }
}

std::string IndexManager::generateIndexKey(const std::string& tableName, const std::string& columnName) const {
    return tableName + "_" + columnName + "_idx";
}

Value IndexManager::extractColumnValue(const Row& row, const std::string& tableName, 
                                      const std::string& columnName) const {
    auto tableIt = tables_.find(tableName);
    if (tableIt == tables_.end()) {
        return Value{}; // 返回默认值
    }
    
    auto table = tableIt->second;
    int columnIndex = table->getColumnIndex(columnName);
    if (columnIndex < 0) {
        return Value{}; // 返回默认值
    }
    
    return row.getValue(columnIndex);
}

bool IndexManager::validateIndexName(const std::string& indexName) const {
    if (indexName.empty()) return false;
    
    // 检查是否包含非法字符
    for (char c : indexName) {
        if (!std::isalnum(c) && c != '_') {
            return false;
        }
    }
    
    return true;
}
