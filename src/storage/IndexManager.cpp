#include "../../include/storage/IndexManager.h"
#include <iostream>
#include <algorithm>
#include <fstream>
#include <sstream>

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
        std::vector<uint32_t> allRecordIds = table->getAllRecordIds();
        
        for (uint32_t recordId : allRecordIds) {
            Row row = table->getRow(recordId);
            if (row.getFieldCount() > 0) {
                Value columnValue = row.getValue(columnIndex);
                
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
        
        // 先查找所有匹配的记录
        auto existingRecords = indexIt->second->search(columnValue);
        
        // 如果索引中没有该键，跳过
        if (existingRecords.empty()) {
            continue;
        }
        
        // 尝试删除所有匹配的记录（解决recordId不一致问题）
        int removedCount = 0;
        for (uint32_t existingRecordId : existingRecords) {
            if (indexIt->second->remove(columnValue, existingRecordId)) {
                removedCount++;
            }
        }
        
        // 如果没有成功删除任何记录，显示警告
        if (removedCount == 0) {
            std::cerr << "Warning: Could not remove any records from index " << indexName 
                      << " (key=" << std::visit([](const auto& v) -> std::string {
                          std::ostringstream oss; oss << v; return oss.str();
                      }, columnValue) << ", found " << existingRecords.size() << " matches)" << std::endl;
        }
    }
    
    return true;
}

bool IndexManager::updateRecord(const std::string& tableName, const Row& oldRow, 
                               const Row& newRow, uint32_t recordId) {
    // 只更新那些索引列值发生变化的索引
    for (const auto& pair : indexInfos_) {
        const auto& indexInfo = pair.second;
        if (indexInfo->tableName != tableName) continue;
        
        const std::string& indexName = pair.first;
        
        // 提取旧值和新值
        Value oldColumnValue = extractColumnValue(oldRow, tableName, indexInfo->columnName);
        Value newColumnValue = extractColumnValue(newRow, tableName, indexInfo->columnName);
        
        // 如果值没有变化，跳过这个索引
        if (oldColumnValue == newColumnValue) {
            continue;
        }
        
        auto indexIt = indexes_.find(indexName);
        if (indexIt == indexes_.end()) continue;
        
        // 删除旧记录
        if (!indexIt->second->remove(oldColumnValue, recordId)) {
            std::cerr << "Failed to remove from index: " << indexName << std::endl;
            return false;
        }
        
        // 插入新记录
        if (!indexIt->second->insert(newColumnValue, recordId)) {
            std::cerr << "Failed to insert into index: " << indexName << std::endl;
            // 尝试回滚
            indexIt->second->insert(oldColumnValue, recordId);
            return false;
        }
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

bool IndexManager::saveIndexes(const std::string& dbPath) const {
    try {
        std::string indexPath = dbPath + "/indexes.meta";
        std::ofstream file(indexPath);
        if (!file.is_open()) {
            std::cerr << "Failed to open index metadata file for writing: " << indexPath << std::endl;
            return false;
        }
        
        // 写入索引数量
        file << indexInfos_.size() << std::endl;
        
        // 写入每个索引的信息
        for (const auto& pair : indexInfos_) {
            const auto& indexInfo = pair.second;
            file << indexInfo->indexName << "|"
                 << indexInfo->tableName << "|"
                 << indexInfo->columnName << "|"
                 << static_cast<int>(indexInfo->indexType) << "|"
                 << (indexInfo->isUnique ? 1 : 0) << std::endl;
        }
        
        file.close();
        
        // 保存每个B+树的数据
        for (const auto& pair : indexes_) {
            const std::string& indexName = pair.first;
            const auto& btree = pair.second;
            
            std::string indexDataPath = dbPath + "/" + indexName + ".index";
            // TODO: 实现B+树的序列化保存
            // btree->saveToDisk(indexDataPath);
        }
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Error saving indexes: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::loadIndexes(const std::string& dbPath) {
    try {
        std::string indexPath = dbPath + "/indexes.meta";
        std::ifstream file(indexPath);
        if (!file.is_open()) {
            // 索引文件不存在，可能是新数据库
            return true;
        }
        
        std::string line;
        
        // 读取索引数量
        if (!std::getline(file, line)) {
            return true; // 空文件
        }
        
        auto indexCount = std::stoull(line);
        
        // 读取每个索引的信息
        for (size_t i = 0; i < indexCount; ++i) {
            if (!std::getline(file, line)) {
                break;
            }
            
            std::vector<std::string> parts;
            size_t pos = 0;
            size_t pipePos;
            
            // 分割字符串
            while ((pipePos = line.find('|', pos)) != std::string::npos) {
                parts.push_back(line.substr(pos, pipePos - pos));
                pos = pipePos + 1;
            }
            parts.push_back(line.substr(pos));
            
            if (parts.size() >= 5) {
                std::string indexName = parts[0];
                std::string tableName = parts[1];
                std::string columnName = parts[2];
                IndexType indexType = static_cast<IndexType>(std::stoi(parts[3]));
                bool isUnique = (std::stoi(parts[4]) != 0);
                
                // 重新创建索引（不创建B+树实例，等表加载完成后再创建）
                auto indexInfo = std::make_unique<IndexInfo>(indexName, tableName, columnName, indexType, isUnique);
                indexInfos_[indexName] = std::move(indexInfo);
            }
        }
        
        file.close();
        
        // 重建B+树索引（需要在所有表加载完成后调用rebuildIndexes）
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Error loading indexes: " << e.what() << std::endl;
        return false;
    }
}

void IndexManager::rebuildIndexes() {
    for (const auto& pair : indexInfos_) {
        const std::string& indexName = pair.first;
        const auto& indexInfo = pair.second;
        
        // 检查表是否存在
        auto tableIt = tables_.find(indexInfo->tableName);
        if (tableIt == tables_.end()) {
            std::cerr << "Warning: Table '" << indexInfo->tableName 
                      << "' not found for index '" << indexName << "'" << std::endl;
            continue;
        }
        
        // 创建B+树实例
        auto btree = std::make_unique<BPlusTree>(10); // 使用默认阶数
        
        // 重建索引数据
        auto table = tableIt->second;
        
        // 获取所有实际的recordId
        std::vector<uint32_t> allRecordIds = table->getAllRecordIds();
        
        for (uint32_t recordId : allRecordIds) {
            Row row = table->getRow(recordId);
            if (row.getFieldCount() > 0) {
                Value columnValue = extractColumnValue(row, indexInfo->tableName, indexInfo->columnName);
                btree->insert(columnValue, recordId);
            }
        }
        
        indexes_[indexName] = std::move(btree);
        
        std::cout << "Index '" << indexName << "' rebuilt successfully" << std::endl;
    }
}

bool IndexManager::rebuildTableIndexes(const std::string& tableName) {
    // 查找表
    auto tableIt = tables_.find(tableName);
    if (tableIt == tables_.end()) {
        std::cerr << "Table not found for index rebuild: " << tableName << std::endl;
        return false;
    }
    
    auto table = tableIt->second;
    bool allSuccess = true;
    
    // 重建该表的所有索引
    for (const auto& pair : indexInfos_) {
        const auto& indexInfo = pair.second;
        if (indexInfo->tableName != tableName) continue;
        
        const std::string& indexName = pair.first;
        auto indexIt = indexes_.find(indexName);
        if (indexIt == indexes_.end()) continue;
        
        std::cerr << "Rebuilding index: " << indexName << std::endl;
        
        // 清空索引
        indexIt->second = std::make_unique<BPlusTree>();
        
        // 重新构建索引
        int columnIndex = table->getColumnIndex(indexInfo->columnName);
        if (columnIndex < 0) {
            std::cerr << "Column not found during rebuild: " << indexInfo->columnName << std::endl;
            allSuccess = false;
            continue;
        }
        
        std::vector<uint32_t> allRecordIds = table->getAllRecordIds();
        for (uint32_t recordId : allRecordIds) {
            Row row = table->getRow(recordId);
            if (row.getFieldCount() > 0) {
                Value columnValue = row.getValue(columnIndex);
                if (!indexIt->second->insert(columnValue, recordId)) {
                    std::cerr << "Failed to insert into rebuilt index: " << indexName << std::endl;
                    allSuccess = false;
                }
            }
        }
        
        std::cerr << "Index " << indexName << " rebuilt with " << allRecordIds.size() << " records" << std::endl;
    }
    
    return allSuccess;
}
