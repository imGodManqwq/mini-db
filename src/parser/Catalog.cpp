#include "../../include/parser/Catalog.h"
#include "../../include/storage/StorageEngine.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>

Catalog::Catalog() : storageEngine_(nullptr) {}

Catalog::Catalog(StorageEngine *storageEngine) : storageEngine_(storageEngine)
{
    if (storageEngine_)
    {
        syncFromStorage();
    }
}

void Catalog::setStorageEngine(StorageEngine *storageEngine)
{
    storageEngine_ = storageEngine;
    if (storageEngine_)
    {
        syncFromStorage();
    }
}

void Catalog::syncFromStorage()
{
    if (!storageEngine_)
    {
        return;
    }

    // 清空现有的表信息
    tables_.clear();

    // 从StorageEngine获取所有表名
    auto tableNames = storageEngine_->getAllTableNames();

    // 为每个表创建元数据
    for (const auto &tableName : tableNames)
    {
        auto table = storageEngine_->getTable(tableName);
        if (table)
        {
            const auto &columns = table->getColumns();
            tables_[tableName] = TableMetadata(tableName, columns);
        }
    }
}

void Catalog::updateFromStorageEngine()
{
    syncFromStorage();
}

bool Catalog::createTable(const std::string &tableName, const std::vector<ColumnInfo> &columns)
{
    if (tableName.empty() || columns.empty())
    {
        return false;
    }

    if (tableExists(tableName))
    {
        return false; // 表已存在
    }

    // 检查列名重复
    std::unordered_map<std::string, bool> columnNames;
    for (const auto &column : columns)
    {
        if (column.name.empty())
        {
            return false; // 列名不能为空
        }

        if (columnNames.find(column.name) != columnNames.end())
        {
            return false; // 列名重复
        }

        columnNames[column.name] = true;

        if (!isValidType(column.type))
        {
            return false; // 无效的数据类型
        }
    }

    // 如果有关联的StorageEngine，使用它来创建表
    if (storageEngine_)
    {
        bool result = storageEngine_->createTable(tableName, columns);
        if (result)
        {
            // 同步更新catalog信息
            tables_[tableName] = TableMetadata(tableName, columns);
        }
        return result;
    }
    else
    {
        // 独立模式，仅在内存中创建
        tables_[tableName] = TableMetadata(tableName, columns);
        return true;
    }
}

bool Catalog::dropTable(const std::string &tableName)
{
    auto it = tables_.find(tableName);
    if (it == tables_.end())
    {
        return false; // 表不存在
    }

    // 如果有关联的StorageEngine，使用它来删除表
    if (storageEngine_)
    {
        bool result = storageEngine_->dropTable(tableName);
        if (result)
        {
            tables_.erase(it);
        }
        return result;
    }
    else
    {
        // 独立模式，仅从内存中删除
        tables_.erase(it);
        return true;
    }
}

bool Catalog::tableExists(const std::string &tableName) const
{
    // 如果有StorageEngine，实时检查
    if (storageEngine_)
    {
        return storageEngine_->tableExists(tableName);
    }
    // 否则查看内存中的信息
    return tables_.find(tableName) != tables_.end();
}

TableMetadata *Catalog::getTableMetadata(const std::string &tableName)
{
    // 如果有StorageEngine，确保信息是最新的
    if (storageEngine_ && storageEngine_->tableExists(tableName))
    {
        auto table = storageEngine_->getTable(tableName);
        if (table)
        {
            const auto &columns = table->getColumns();
            tables_[tableName] = TableMetadata(tableName, columns);
        }
    }

    auto it = tables_.find(tableName);
    return (it != tables_.end()) ? &it->second : nullptr;
}

const TableMetadata *Catalog::getTableMetadata(const std::string &tableName) const
{
    auto it = tables_.find(tableName);
    return (it != tables_.end()) ? &it->second : nullptr;
}

bool Catalog::columnExists(const std::string &tableName, const std::string &columnName) const
{
    const auto *metadata = getTableMetadata(tableName);
    if (!metadata)
    {
        return false;
    }

    for (const auto &column : metadata->columns)
    {
        if (column.name == columnName)
        {
            return true;
        }
    }

    return false;
}

DataType Catalog::getColumnType(const std::string &tableName, const std::string &columnName) const
{
    const auto *metadata = getTableMetadata(tableName);
    if (!metadata)
    {
        throw std::runtime_error("Table '" + tableName + "' does not exist");
    }

    for (const auto &column : metadata->columns)
    {
        if (column.name == columnName)
        {
            return column.type;
        }
    }

    throw std::runtime_error("Column '" + columnName + "' does not exist in table '" + tableName + "'");
}

int Catalog::getColumnIndex(const std::string &tableName, const std::string &columnName) const
{
    const auto *metadata = getTableMetadata(tableName);
    if (!metadata)
    {
        return -1;
    }

    for (size_t i = 0; i < metadata->columns.size(); ++i)
    {
        if (metadata->columns[i].name == columnName)
        {
            return static_cast<int>(i);
        }
    }

    return -1;
}

const std::vector<ColumnInfo> &Catalog::getTableColumns(const std::string &tableName) const
{
    const auto *metadata = getTableMetadata(tableName);
    if (!metadata)
    {
        throw std::runtime_error("Table '" + tableName + "' does not exist");
    }

    return metadata->columns;
}

bool Catalog::isValidType(DataType type) const
{
    return type == DataType::INT || type == DataType::STRING || type == DataType::DOUBLE;
}

bool Catalog::isTypeCompatible(DataType columnType, DataType valueType) const
{
    // 完全匹配
    if (columnType == valueType)
    {
        return true;
    }

    // 数值类型之间的兼容性
    if ((columnType == DataType::DOUBLE && valueType == DataType::INT) ||
        (columnType == DataType::INT && valueType == DataType::DOUBLE))
    {
        return true;
    }

    return false;
}

bool Catalog::validateValue(const Value &value, DataType expectedType) const
{
    try
    {
        switch (expectedType)
        {
        case DataType::INT:
            if (std::holds_alternative<int>(value))
            {
                return true;
            }
            if (std::holds_alternative<double>(value))
            {
                double d = std::get<double>(value);
                return d == static_cast<int>(d); // 检查是否为整数
            }
            return false;

        case DataType::DOUBLE:
            return std::holds_alternative<int>(value) || std::holds_alternative<double>(value);

        case DataType::STRING:
            return std::holds_alternative<std::string>(value);

        default:
            return false;
        }
    }
    catch (...)
    {
        return false;
    }
}

Value Catalog::convertValue(const Value &value, DataType targetType) const
{
    try
    {
        switch (targetType)
        {
        case DataType::INT:
            if (std::holds_alternative<int>(value))
            {
                return value;
            }
            if (std::holds_alternative<double>(value))
            {
                return static_cast<int>(std::get<double>(value));
            }
            if (std::holds_alternative<std::string>(value))
            {
                return std::stoi(std::get<std::string>(value));
            }
            break;

        case DataType::DOUBLE:
            if (std::holds_alternative<double>(value))
            {
                return value;
            }
            if (std::holds_alternative<int>(value))
            {
                return static_cast<double>(std::get<int>(value));
            }
            if (std::holds_alternative<std::string>(value))
            {
                return std::stod(std::get<std::string>(value));
            }
            break;

        case DataType::STRING:
            if (std::holds_alternative<std::string>(value))
            {
                return value;
            }
            if (std::holds_alternative<int>(value))
            {
                return std::to_string(std::get<int>(value));
            }
            if (std::holds_alternative<double>(value))
            {
                return std::to_string(std::get<double>(value));
            }
            break;
        }
    }
    catch (...)
    {
        throw std::runtime_error("Cannot convert value to target type");
    }

    throw std::runtime_error("Cannot convert value to target type");
}

void Catalog::printCatalog() const
{
    std::cout << "=== Database Catalog ===" << std::endl;
    std::cout << "Total tables: " << tables_.size() << std::endl;

    for (const auto &[tableName, metadata] : tables_)
    {
        std::cout << "\nTable: " << tableName << std::endl;
        std::cout << "Columns (" << metadata.columns.size() << "):" << std::endl;

        for (size_t i = 0; i < metadata.columns.size(); ++i)
        {
            const auto &column = metadata.columns[i];
            std::cout << "  " << i << ". " << column.name
                      << " (" << dataTypeToString(column.type) << ")" << std::endl;
        }
    }
}

std::vector<std::string> Catalog::getTableNames() const
{
    std::vector<std::string> names;
    names.reserve(tables_.size());

    for (const auto &[tableName, _] : tables_)
    {
        names.push_back(tableName);
    }

    return names;
}

size_t Catalog::getTableCount() const
{
    return tables_.size();
}

bool Catalog::saveToFile(const std::string &filename) const
{
    try
    {
        std::ofstream file(filename);
        if (!file.is_open())
        {
            return false;
        }

        file << tables_.size() << std::endl;

        for (const auto &[tableName, metadata] : tables_)
        {
            file << tableName << std::endl;
            file << metadata.columns.size() << std::endl;

            for (const auto &column : metadata.columns)
            {
                file << column.name << " " << static_cast<int>(column.type) << std::endl;
            }
        }

        return true;
    }
    catch (...)
    {
        return false;
    }
}

bool Catalog::loadFromFile(const std::string &filename)
{
    try
    {
        std::ifstream file(filename);
        if (!file.is_open())
        {
            return false;
        }

        tables_.clear();

        size_t tableCount;
        file >> tableCount;

        for (size_t i = 0; i < tableCount; ++i)
        {
            std::string tableName;
            file >> tableName;

            size_t columnCount;
            file >> columnCount;

            std::vector<ColumnInfo> columns;
            columns.reserve(columnCount);

            for (size_t j = 0; j < columnCount; ++j)
            {
                std::string columnName;
                int typeInt;
                file >> columnName >> typeInt;

                columns.emplace_back(columnName, static_cast<DataType>(typeInt));
            }

            tables_[tableName] = TableMetadata(tableName, columns);
        }

        return true;
    }
    catch (...)
    {
        return false;
    }
}

std::string Catalog::dataTypeToString(DataType type) const
{
    switch (type)
    {
    case DataType::INT:
        return "INT";
    case DataType::STRING:
        return "STRING";
    case DataType::DOUBLE:
        return "DOUBLE";
    default:
        return "UNKNOWN";
    }
}

DataType Catalog::stringToDataType(const std::string &typeStr) const
{
    if (typeStr == "INT")
        return DataType::INT;
    if (typeStr == "STRING")
        return DataType::STRING;
    if (typeStr == "DOUBLE")
        return DataType::DOUBLE;
    throw std::runtime_error("Unknown data type: " + typeStr);
}
