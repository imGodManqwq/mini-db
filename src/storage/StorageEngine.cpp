#include "../../include/storage/StorageEngine.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>

StorageEngine::StorageEngine(const std::string& dbPath) : dbPath_(dbPath), nextRecordId_(1) {
    // 确保数据库目录存在
    std::filesystem::create_directories(dbPath);
    
    // 初始化页面管理器
    std::string dbFile = dbPath + "/database.db";
    pageManager_ = std::make_unique<PageManager>(dbFile);
    
    // 初始化索引管理器
    indexManager_ = std::make_unique<IndexManager>();
    
    // 加载元数据
    loadFromStorage();
}

StorageEngine::~StorageEngine() {
    saveToStorage();
}

bool StorageEngine::createTable(const std::string& tableName, const std::vector<ColumnInfo>& columns) {
    if (tableExists(tableName)) {
        std::cerr << "Table '" << tableName << "' already exists" << std::endl;
        return false;
    }
    
    auto table = std::make_shared<Table>(tableName, columns, pageManager_.get());
    tables_[tableName] = table;
    
    // 注册表到索引管理器
    indexManager_->registerTable(table);
    
    // 为PRIMARY KEY自动创建唯一索引
    for (const auto& column : columns) {
        if (column.isPrimaryKey) {
            std::string pkIndexName = "pk_" + tableName + "_" + column.name;
            bool indexCreated = indexManager_->createIndex(pkIndexName, tableName, column.name, IndexType::BTREE, true);
            if (indexCreated) {
                std::cout << "Primary key index '" << pkIndexName << "' created automatically" << std::endl;
            } else {
                std::cerr << "Warning: Failed to create primary key index for column '" << column.name << "'" << std::endl;
            }
            break; // 只应该有一个主键
        }
    }
    
    std::cout << "Table '" << tableName << "' created successfully" << std::endl;
    return true;
}

bool StorageEngine::dropTable(const std::string& tableName) {
    auto it = tables_.find(tableName);
    if (it == tables_.end()) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return false;
    }
    
    // 从索引管理器中注销表
    indexManager_->unregisterTable(tableName);
    
    tables_.erase(it);
    
    // 删除表文件
    std::string tableFile = getTableFileName(tableName);
    std::filesystem::remove(tableFile);
    
    std::cout << "Table '" << tableName << "' dropped successfully" << std::endl;
    return true;
}

std::shared_ptr<Table> StorageEngine::getTable(const std::string& tableName) {
    auto it = tables_.find(tableName);
    return (it != tables_.end()) ? it->second : nullptr;
}

std::shared_ptr<const Table> StorageEngine::getTable(const std::string& tableName) const {
    auto it = tables_.find(tableName);
    return (it != tables_.end()) ? it->second : nullptr;
}

bool StorageEngine::tableExists(const std::string& tableName) const {
    return tables_.find(tableName) != tables_.end();
}

std::vector<std::string> StorageEngine::getAllTableNames() const {
    std::vector<std::string> names;
    for (const auto& pair : tables_) {
        names.push_back(pair.first);
    }
    return names;
}

bool StorageEngine::insertRow(const std::string& tableName, const Row& row) {
    auto table = getTable(tableName);
    if (!table) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return false;
    }
    
    try {
        // 插入到表中（现在会使用页面管理）
        uint32_t recordId = table->insertRow(row);
        
        // 更新索引
        if (!indexManager_->insertRecord(tableName, row, recordId)) {
            // 索引插入失败，需要回滚
            table->deleteRow(recordId);
            std::cerr << "Failed to update indexes for inserted row" << std::endl;
            return false;
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error inserting row: " << e.what() << std::endl;
        return false;
    }
}

bool StorageEngine::insertRow(const std::string& tableName, const std::vector<Value>& values) {
    Row row(values);
    return insertRow(tableName, row);
}

size_t StorageEngine::batchInsertRows(const std::string& tableName, const std::vector<std::vector<Value>>& batchData) {
    auto table = getTable(tableName);
    if (!table) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return 0;
    }
    
    size_t successCount = 0;
    
    try {
        // 批量插入，直接使用现有的insertRow方法以保证索引一致性
        for (const auto& values : batchData) {
            if (insertRow(tableName, values)) {
                successCount++;
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Batch insert failed: " << e.what() << std::endl;
    }
    
    return successCount;
}

size_t StorageEngine::fastBatchInsertRows(const std::string& tableName, const std::vector<std::vector<Value>>& batchData) {
    auto table = getTable(tableName);
    if (!table) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return 0;
    }
    
    size_t successCount = 0;
    
    try {
        // 超快速批量插入：跳过索引更新、约束检查、立即写盘
        for (const auto& values : batchData) {
            Row row(values);
            
            // 使用超快速插入方法
            uint32_t recordId = table->fastInsertRow(row);
            successCount++;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Fast batch insert failed: " << e.what() << std::endl;
    }
    
    return successCount;
}

void StorageEngine::rebuildTableIndexes(const std::string& tableName) {
    auto table = getTable(tableName);
    if (!table) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return;
    }
    
    std::cout << "Rebuilding indexes for table '" << tableName << "'..." << std::endl;
    
    // 获取表的所有索引
    auto indexNames = indexManager_->getIndexesForTable(tableName);
    
    for (const auto& indexName : indexNames) {
        std::cout << "Rebuilding index: " << indexName << std::endl;
        
        // 删除并重新创建索引
        const auto* indexInfo = indexManager_->getIndexInfo(indexName);
        if (indexInfo) {
            // 保存索引信息
            std::string columnName = indexInfo->columnName;
            bool isUnique = indexInfo->isUnique;
            
            // 删除旧索引
            indexManager_->dropIndex(indexName);
            
            // 重新创建索引
            bool created = indexManager_->createIndex(indexName, tableName, columnName, IndexType::BTREE, isUnique);
            if (created) {
                std::cout << "v Index '" << indexName << "' rebuilt successfully" << std::endl;
            } else {
                std::cerr << "x Failed to rebuild index '" << indexName << "'" << std::endl;
            }
        }
    }
    
    std::cout << "Index rebuilding completed for table '" << tableName << "'" << std::endl;
}

void StorageEngine::flushAllPages() {
    if (pageManager_) {
        std::cout << "Flushing all dirty pages to disk..." << std::endl;
        pageManager_->flushAllPages();
        std::cout << "All pages flushed successfully" << std::endl;
    }
}

bool StorageEngine::indexExists(const std::string& indexName) const {
    if (!indexManager_) {
        return false;
    }
    
    // 使用 IndexManager 的内部方法检查索引是否存在
    // 我们可以通过尝试获取索引信息来判断
    const auto* indexInfo = indexManager_->getIndexInfo(indexName);
    return indexInfo != nullptr;
}

bool StorageEngine::deleteRow(const std::string& tableName, const Row& row, uint32_t recordId) {
    auto table = getTable(tableName);
    if (!table) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return false;
    }
    
    // 先从索引中删除，因为删除表记录后recordId可能失效
    bool indexDeleteSuccess = indexManager_->deleteRecord(tableName, row, recordId);
    if (!indexDeleteSuccess) {
        std::cerr << "Warning: Failed to remove from indexes for record " << recordId << std::endl;
    }
    
    // 然后从表中删除行
    if (!table->deleteRow(recordId)) {
        // 检查是否是因为recordId不存在（可能已经被删除）
        Row testRow = table->getRow(recordId);
        if (testRow.getFieldCount() == 0) {
            // recordId不存在，可能已经被删除，这不算错误
            std::cout << "Row deleted from table " << tableName << " (already removed)" << std::endl;
            return true;
        } else {
            std::cerr << "Failed to delete row from table" << std::endl;
            return false;
        }
    }
    
    std::cout << "Row deleted from table " << tableName << std::endl;
    return true;
}

bool StorageEngine::updateRow(const std::string& tableName, const Row& oldRow, const Row& newRow, uint32_t recordId) {
    auto table = getTable(tableName);
    if (!table) {
        std::cerr << "Table '" << tableName << "' does not exist" << std::endl;
        return false;
    }
    
    // 先更新表中的数据
    if (!table->updateRow(recordId, newRow)) {
        std::cerr << "Failed to update row in table" << std::endl;
        return false;
    }
    
    // 更新索引
    if (!indexManager_->updateRecord(tableName, oldRow, newRow, recordId)) {
        std::cerr << "Failed to update indexes" << std::endl;
        // 这里理想情况下应该回滚表的更新，但为简化先不实现
        return false;
    }
    
    std::cout << "Row updated successfully in table " << tableName << std::endl;
    return true;
}

bool StorageEngine::createIndex(const std::string& indexName, const std::string& tableName, 
                               const std::string& columnName, bool isUnique) {
    return indexManager_->createIndex(indexName, tableName, columnName, IndexType::BTREE, isUnique);
}

bool StorageEngine::dropIndex(const std::string& indexName) {
    return indexManager_->dropIndex(indexName);
}

std::vector<uint32_t> StorageEngine::searchByIndex(const std::string& indexName, const Value& key) {
    return indexManager_->searchByIndex(indexName, key);
}

std::vector<uint32_t> StorageEngine::rangeSearchByIndex(const std::string& indexName, 
                                                       const Value& startKey, const Value& endKey) {
    return indexManager_->rangeSearchByIndex(indexName, startKey, endKey);
}

std::vector<uint32_t> StorageEngine::searchByColumn(const std::string& tableName, 
                                                    const std::string& columnName, const Value& key) {
    // 首先检查是否有索引可用
    if (indexManager_->hasIndex(tableName, columnName)) {
        // 找到对应的索引名
        auto indexes = indexManager_->getIndexesForTable(tableName);
        for (const auto& indexName : indexes) {
            const auto* indexInfo = indexManager_->getIndexInfo(indexName);
            if (indexInfo && indexInfo->columnName == columnName) {
                std::cout << "Using index " << indexName << " for search" << std::endl;
                return indexManager_->searchByIndex(indexName, key);
            }
        }
    }
    
    // 没有索引，使用全表扫描
    std::cout << "No index available, using full table scan" << std::endl;
    std::vector<uint32_t> result;
    
    auto table = getTable(tableName);
    if (!table) return result;
    
    int columnIndex = table->getColumnIndex(columnName);
    if (columnIndex < 0) return result;
    
    uint32_t recordId = 0;
    for (auto it = table->begin(); it != table->end(); ++it, ++recordId) {
        if (it->getValue(columnIndex) == key) {
            result.push_back(recordId);
        }
    }
    
    return result;
}

bool StorageEngine::saveToStorage() {
    // 保存元数据
    if (!saveMetadata()) {
        return false;
    }
    
    // 保存每个表的数据
    for (const auto& pair : tables_) {
        std::string tableFile = getTableFileName(pair.first);
        std::ofstream file(tableFile, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open file for table: " << pair.first << std::endl;
            continue;
        }
        
        std::string serializedData = pair.second->serialize();
        file.write(serializedData.c_str(), serializedData.size());
        file.close();
    }
    
    // 保存页面管理器数据
    pageManager_->saveToDisk();
    
    // 保存索引数据
    indexManager_->saveIndexes(dbPath_);
    
    return true;
}

bool StorageEngine::loadFromStorage() {
    // 加载元数据
    if (!loadMetadata()) {
        // 如果没有元数据文件，说明是新数据库
        return true;
    }
    
    // 加载页面管理器数据
    pageManager_->loadFromDisk();
    
    // 加载索引数据（在表加载完成后）
    indexManager_->loadIndexes(dbPath_);
    
    // 重建索引结构
    indexManager_->rebuildIndexes();
    
    return true;
}

void StorageEngine::printStorageInfo() const {
    std::cout << "Storage Engine Information:" << std::endl;
    std::cout << "  Database path: " << dbPath_ << std::endl;
    std::cout << "  Number of tables: " << tables_.size() << std::endl;
    
    if (pageManager_) {
        pageManager_->printStatistics();
    }
    
    std::cout << "  Tables:" << std::endl;
    for (const auto& pair : tables_) {
        std::cout << "    - " << pair.first 
                  << " (" << pair.second->getRowCount() << " rows)" << std::endl;
    }
}

void StorageEngine::printTableInfo(const std::string& tableName) const {
    auto table = getTable(tableName);
    if (table) {
        table->printSchema();
        table->printData();
        
        // 打印该表的索引信息
        auto indexes = indexManager_->getIndexesForTable(tableName);
        if (!indexes.empty()) {
            std::cout << "Indexes on table " << tableName << ":" << std::endl;
            for (const auto& indexName : indexes) {
                const auto* indexInfo = indexManager_->getIndexInfo(indexName);
                if (indexInfo) {
                    std::cout << "  - " << indexName << " on column " << indexInfo->columnName;
                    if (indexInfo->isUnique) {
                        std::cout << " [UNIQUE]";
                    }
                    std::cout << std::endl;
                }
            }
        }
    } else {
        std::cout << "Table '" << tableName << "' does not exist" << std::endl;
    }
}

void StorageEngine::printIndexInfo() const {
    indexManager_->printIndexStats();
}

void StorageEngine::printIndexInfo(const std::string& indexName) const {
    indexManager_->printIndexInfo(indexName);
}

bool StorageEngine::saveMetadata() {
    std::string metaFile = getMetadataFileName();
    std::ofstream file(metaFile);
    if (!file.is_open()) {
        std::cerr << "Failed to open metadata file for writing" << std::endl;
        return false;
    }
    
    // 保存表数量
    file << tables_.size() << std::endl;
    
    // 保存每个表的基本信息
    for (const auto& pair : tables_) {
        const auto& table = pair.second;
        file << table->getTableName() << std::endl;
        file << table->getColumnCount() << std::endl;
        
        for (const auto& col : table->getColumns()) {
            file << col.name << "|" << static_cast<int>(col.type) 
                 << "|" << (col.isNotNull ? 1 : 0) 
                 << "|" << (col.isPrimaryKey ? 1 : 0) << std::endl;
        }
    }
    
    file.close();
    return true;
}

bool StorageEngine::loadMetadata() {
    std::string metaFile = getMetadataFileName();
    std::ifstream file(metaFile);
    if (!file.is_open()) {
        return false; // 元数据文件不存在
    }
    
    std::string line;
    
    // 读取表数量
    if (!std::getline(file, line)) {
        return false;
    }
    auto tableCount = std::stoull(line);
    
    // 读取每个表的信息
    for (size_t i = 0; i < tableCount; ++i) {
        // 读取表名
        if (!std::getline(file, line)) {
            break;
        }
        std::string tableName = line;
        
        // 读取列数量
        if (!std::getline(file, line)) {
            break;
        }
        auto columnCount = std::stoull(line);
        
        // 读取列信息
        std::vector<ColumnInfo> columns;
        for (size_t j = 0; j < columnCount; ++j) {
            if (!std::getline(file, line)) {
                break;
            }
            
            // 解析列信息
            std::vector<std::string> parts;
            size_t pos = 0;
            size_t pipePos;
            
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
        
        // 创建表
        auto table = std::make_shared<Table>(tableName, columns, pageManager_.get());
        
        // 加载表数据
        std::string tableFile = getTableFileName(tableName);
        std::ifstream dataFile(tableFile, std::ios::binary);
        if (dataFile.is_open()) {
            std::string serializedData((std::istreambuf_iterator<char>(dataFile)),
                                     std::istreambuf_iterator<char>());
            dataFile.close();
            
            if (!serializedData.empty()) {
                *table = Table::deserialize(serializedData);
            }
        }
        
        tables_[tableName] = table;
        
        // 注册表到索引管理器
        indexManager_->registerTable(table);
    }
    
    file.close();
    return true;
}

std::string StorageEngine::getMetadataFileName() const {
    return dbPath_ + "/metadata.meta";
}

std::string StorageEngine::getTableFileName(const std::string& tableName) const {
    return dbPath_ + "/" + tableName + ".tbl";
}
