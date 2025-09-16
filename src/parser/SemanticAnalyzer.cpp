#include "../../include/parser/SemanticAnalyzer.h"
#include "../../include/parser/Token.h"
#include <iostream>
#include <sstream>
#include <algorithm>

std::string SemanticError::toString() const
{
    std::stringstream ss;
    ss << "Semantic Error";
    if (line > 0)
    {
        ss << " at line " << line;
        if (column > 0)
        {
            ss << ", column " << column;
        }
    }
    if (!location.empty())
    {
        ss << " in " << location;
    }
    ss << ": " << message;
    return ss.str();
}

SemanticAnalyzer::SemanticAnalyzer(std::shared_ptr<Catalog> catalog)
    : catalog_(catalog) {}

SemanticAnalysisResult SemanticAnalyzer::analyzeStatement(Statement *stmt)
{
    result_ = SemanticAnalysisResult();

    if (!stmt)
    {
        addError(SemanticErrorType::UNKNOWN_ERROR, "Null statement provided");
        return result_;
    }

    // 使用访问者模式分析语句
    stmt->accept(this);

    return result_;
}

SemanticAnalysisResult SemanticAnalyzer::analyzeStatements(const std::vector<std::unique_ptr<Statement>> &statements)
{
    result_ = SemanticAnalysisResult();

    for (const auto &stmt : statements)
    {
        if (stmt)
        {
            stmt->accept(this);
        }
        else
        {
            addError(SemanticErrorType::UNKNOWN_ERROR, "Null statement in statement list");
        }
    }

    return result_;
}

DataType SemanticAnalyzer::inferExpressionType(Expression *expr, const std::string &contextTable)
{
    if (!expr)
    {
        return DataType::INT; // 默认类型
    }

    switch (expr->nodeType)
    {
    case ASTNodeType::LITERAL_EXPR:
    {
        auto *literal = static_cast<LiteralExpression *>(expr);
        if (std::holds_alternative<int>(literal->value))
        {
            return DataType::INT;
        }
        else if (std::holds_alternative<double>(literal->value))
        {
            return DataType::DOUBLE;
        }
        else if (std::holds_alternative<std::string>(literal->value))
        {
            return DataType::STRING;
        }
        break;
    }

    case ASTNodeType::IDENTIFIER_EXPR:
    {
        auto *identifier = static_cast<IdentifierExpression *>(expr);
        std::string tableName = identifier->tableName.empty() ? contextTable : identifier->tableName;

        if (tableName.empty())
        {
            return DataType::INT; // 默认类型
        }

        try
        {
            return catalog_->getColumnType(tableName, identifier->name);
        }
        catch (...)
        {
            return DataType::INT; // 默认类型
        }
    }

    case ASTNodeType::BINARY_EXPR:
    {
        auto *binary = static_cast<BinaryExpression *>(expr);
        DataType leftType = inferExpressionType(binary->left.get(), contextTable);
        DataType rightType = inferExpressionType(binary->right.get(), contextTable);
        return getResultType(leftType, rightType, binary->operator_);
    }

    case ASTNodeType::UNARY_EXPR:
    {
        auto *unary = static_cast<UnaryExpression *>(expr);
        return inferExpressionType(unary->operand.get(), contextTable);
    }

    case ASTNodeType::AGGREGATE_EXPR:
    {
        auto *aggregate = static_cast<AggregateExpression *>(expr);
        switch (aggregate->function)
        {
        case TokenType::COUNT:
            return DataType::INT; // COUNT总是返回整数
        case TokenType::SUM:
            // SUM的返回类型取决于参数类型
            return inferExpressionType(aggregate->argument.get(), contextTable);
        case TokenType::AVG:
            return DataType::DOUBLE; // AVG总是返回浮点数
        case TokenType::MAX:
        case TokenType::MIN:
            // MAX/MIN的返回类型与参数类型相同
            return inferExpressionType(aggregate->argument.get(), contextTable);
        default:
            return DataType::INT;
        }
    }

    default:
        break;
    }

    return DataType::INT; // 默认类型
}

bool SemanticAnalyzer::validateValue(const Value &value, DataType expectedType)
{
    return catalog_->validateValue(value, expectedType);
}

void SemanticAnalyzer::visit(LiteralExpression *node)
{
    // 字面量表达式无需特殊检查
}

void SemanticAnalyzer::visit(IdentifierExpression *node)
{
    analyzeIdentifier(node, currentTable_);
}

void SemanticAnalyzer::visit(BinaryExpression *node)
{
    analyzeBinaryExpression(node, currentTable_);
}

void SemanticAnalyzer::visit(UnaryExpression *node)
{
    if (node->operand)
    {
        analyzeExpression(node->operand.get(), currentTable_);
    }
}

void SemanticAnalyzer::visit(AggregateExpression *node)
{
    // 验证聚合函数的使用
    if (node->argument)
    {
        analyzeExpression(node->argument.get(), currentTable_);
    }

    // 检查聚合函数是否在SELECT子句中使用
    // 这里可以添加更复杂的聚合函数语义检查
    switch (node->function)
    {
    case TokenType::COUNT:
    case TokenType::SUM:
    case TokenType::AVG:
    case TokenType::MAX:
    case TokenType::MIN:
        // 聚合函数是有效的
        break;
    default:
        addError(SemanticErrorType::INVALID_FUNCTION, "Invalid aggregate function");
        break;
    }
}

void SemanticAnalyzer::visit(JoinClause *node)
{
    // 验证右表是否存在
    if (!catalog_->tableExists(node->rightTable))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + node->rightTable + "' does not exist");
        return;
    }

    // 验证ON条件中的列是否存在
    if (node->onCondition)
    {
        // 对于JOIN条件，我们需要检查左表和右表的列
        // 保存原来的表上下文
        std::string oldTable = currentTable_;

        // 分析JOIN条件时，我们需要特殊处理，允许引用两个表
        analyzeJoinCondition(node->onCondition.get(), currentTable_, node->rightTable);

        // 恢复原来的表上下文
        currentTable_ = oldTable;
    }
}

void SemanticAnalyzer::visit(ColumnDefinition *node)
{
    if (node->columnName.empty())
    {
        addError(SemanticErrorType::EMPTY_COLUMN_NAME, "Column name cannot be empty");
    }

    if (!catalog_->isValidType(node->dataType))
    {
        addError(SemanticErrorType::INVALID_DATA_TYPE,
                 "Invalid data type for column '" + node->columnName + "'");
    }
}

void SemanticAnalyzer::visit(CreateTableStatement *node)
{
    analyzeCreateTable(node);
}

void SemanticAnalyzer::visit(DropTableStatement *node)
{
    analyzeDropTable(node);
}

void SemanticAnalyzer::visit(InsertStatement *node)
{
    analyzeInsert(node);
}

void SemanticAnalyzer::visit(SelectStatement *node)
{
    analyzeSelect(node);
}

void SemanticAnalyzer::visit(DeleteStatement *node)
{
    analyzeDelete(node);
}

void SemanticAnalyzer::visit(UpdateStatement *node)
{
    analyzeUpdate(node);
}

void SemanticAnalyzer::visit(CreateIndexStatement *node)
{
    analyzeCreateIndex(node);
}

void SemanticAnalyzer::printErrors() const
{
    for (const auto &error : result_.errors)
    {
        std::cerr << error.toString() << std::endl;
    }

    for (const auto &warning : result_.warnings)
    {
        std::cout << "Warning: " << warning << std::endl;
    }
}

void SemanticAnalyzer::addError(SemanticErrorType type, const std::string &message, const std::string &location)
{
    result_.addError(SemanticError(type, message, 0, 0, location));
}

void SemanticAnalyzer::addWarning(const std::string &warning)
{
    result_.addWarning(warning);
}

void SemanticAnalyzer::analyzeCreateTable(CreateTableStatement *stmt)
{
    currentTable_ = stmt->tableName;

    // 检查表名
    if (stmt->tableName.empty())
    {
        addError(SemanticErrorType::EMPTY_TABLE_NAME, "Table name cannot be empty");
        return;
    }

    // 检查表是否已存在
    if (catalog_->tableExists(stmt->tableName))
    {
        addError(SemanticErrorType::TABLE_ALREADY_EXISTS,
                 "Table '" + stmt->tableName + "' already exists");
        return;
    }

    // 检查列定义
    if (stmt->columns.empty())
    {
        addError(SemanticErrorType::UNKNOWN_ERROR, "Table must have at least one column");
        return;
    }

    std::unordered_map<std::string, bool> columnNames;
    bool hasPrimaryKey = false;

    for (const auto &columnDef : stmt->columns)
    {
        // 检查列定义
        columnDef->accept(this);

        // 检查列名重复
        if (columnNames.find(columnDef->columnName) != columnNames.end())
        {
            addError(SemanticErrorType::DUPLICATE_COLUMN_NAME,
                     "Duplicate column name '" + columnDef->columnName + "'");
        }
        else
        {
            columnNames[columnDef->columnName] = true;
        }

        // 检查主键
        if (columnDef->isPrimaryKey)
        {
            if (hasPrimaryKey)
            {
                addError(SemanticErrorType::DUPLICATE_PRIMARY_KEY,
                         "Multiple primary keys are not allowed");
            }
            else
            {
                hasPrimaryKey = true;
            }
        }
    }
}

void SemanticAnalyzer::analyzeDropTable(DropTableStatement *stmt)
{
    currentTable_ = stmt->tableName;

    // 检查表名
    if (stmt->tableName.empty())
    {
        addError(SemanticErrorType::EMPTY_TABLE_NAME, "Table name cannot be empty");
        return;
    }

    // 如果不是 IF EXISTS 模式，检查表是否存在
    if (!stmt->ifExists)
    {
        if (!catalog_->tableExists(stmt->tableName))
        {
            addError(SemanticErrorType::TABLE_NOT_EXISTS,
                     "Table '" + stmt->tableName + "' does not exist");
            return;
        }
    }
    // 如果是 IF EXISTS 模式，不管表是否存在都不报错
}

void SemanticAnalyzer::analyzeInsert(InsertStatement *stmt)
{
    currentTable_ = stmt->tableName;

    // 检查表是否存在
    if (!catalog_->tableExists(stmt->tableName))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + stmt->tableName + "' does not exist");
        return;
    }

    const auto &tableColumns = catalog_->getTableColumns(stmt->tableName);

    // 检查指定的列是否存在
    if (!stmt->columns.empty())
    {
        for (const auto &columnName : stmt->columns)
        {
            if (!catalog_->columnExists(stmt->tableName, columnName))
            {
                addError(SemanticErrorType::COLUMN_NOT_EXISTS,
                         "Column '" + columnName + "' does not exist in table '" + stmt->tableName + "'");
            }
        }
    }

    // 检查值的数量和类型
    for (size_t i = 0; i < stmt->valuesList.size(); ++i)
    {
        const auto &values = stmt->valuesList[i];

        size_t expectedColumnCount;
        if (stmt->columns.empty())
        {
            expectedColumnCount = tableColumns.size();
        }
        else
        {
            expectedColumnCount = stmt->columns.size();
        }

        if (values.size() != expectedColumnCount)
        {
            addError(SemanticErrorType::COLUMN_COUNT_MISMATCH,
                     "Value count mismatch in row " + std::to_string(i + 1) +
                         ": expected " + std::to_string(expectedColumnCount) +
                         ", got " + std::to_string(values.size()));
            continue;
        }

        // 检查每个值的类型
        for (size_t j = 0; j < values.size(); ++j)
        {
            DataType expectedType;
            std::string columnName;

            if (stmt->columns.empty())
            {
                expectedType = tableColumns[j].type;
                columnName = tableColumns[j].name;
            }
            else
            {
                int columnIndex = catalog_->getColumnIndex(stmt->tableName, stmt->columns[j]);
                if (columnIndex >= 0)
                {
                    expectedType = tableColumns[columnIndex].type;
                    columnName = stmt->columns[j];
                }
                else
                {
                    continue; // 错误已在前面报告
                }
            }

            // 分析表达式
            analyzeExpression(values[j].get(), stmt->tableName);

            // 检查类型兼容性
            if (auto *literal = dynamic_cast<LiteralExpression *>(values[j].get()))
            {
                if (!validateValue(literal->value, expectedType))
                {
                    addError(SemanticErrorType::TYPE_MISMATCH,
                             "Type mismatch for column '" + columnName + "' in row " +
                                 std::to_string(i + 1) + ", value " + std::to_string(j + 1));
                }
            }
        }
    }
}

void SemanticAnalyzer::analyzeSelect(SelectStatement *stmt)
{
    currentTable_ = stmt->fromTable;

    // 检查表是否存在
    if (!catalog_->tableExists(stmt->fromTable))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + stmt->fromTable + "' does not exist");
        return;
    }

    // 检查SELECT列表
    for (const auto &selectExpr : stmt->selectList)
    {
        analyzeExpression(selectExpr.get(), stmt->fromTable);
    }

    // 检查WHERE子句
    if (stmt->whereClause)
    {
        analyzeExpression(stmt->whereClause.get(), stmt->fromTable);
    }

    // 检查GROUP BY子句
    if (!stmt->groupByList.empty())
    {
        for (const auto &groupExpr : stmt->groupByList)
        {
            analyzeExpression(groupExpr.get(), stmt->fromTable);
        }
    }

    // 检查ORDER BY子句
    if (!stmt->orderByList.empty())
    {
        for (const auto &orderItem : stmt->orderByList)
        {
            if (orderItem->expression)
            {
                analyzeExpression(orderItem->expression.get(), stmt->fromTable);
            }
        }
    }

    // 检查JOIN子句
    if (!stmt->joinClauses.empty())
    {
        for (const auto &joinClause : stmt->joinClauses)
        {
            if (joinClause)
            {
                joinClause->accept(this);
            }
        }
    }
}

void SemanticAnalyzer::analyzeDelete(DeleteStatement *stmt)
{
    currentTable_ = stmt->tableName;

    // 检查表是否存在
    if (!catalog_->tableExists(stmt->tableName))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + stmt->tableName + "' does not exist");
        return;
    }

    // 检查WHERE子句
    if (stmt->whereClause)
    {
        analyzeExpression(stmt->whereClause.get(), stmt->tableName);
    }
}

void SemanticAnalyzer::analyzeUpdate(UpdateStatement *stmt)
{
    currentTable_ = stmt->tableName;

    // 检查表是否存在
    if (!catalog_->tableExists(stmt->tableName))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + stmt->tableName + "' does not exist");
        return;
    }

    // 检查SET子句中的赋值
    if (stmt->assignments.empty())
    {
        addError(SemanticErrorType::UNKNOWN_ERROR,
                 "UPDATE statement must have at least one assignment");
        return;
    }

    for (const auto &assignment : stmt->assignments)
    {
        // 检查列是否存在
        if (!catalog_->columnExists(stmt->tableName, assignment->columnName))
        {
            addError(SemanticErrorType::COLUMN_NOT_EXISTS,
                     "Column '" + assignment->columnName + "' does not exist in table '" + stmt->tableName + "'");
            continue;
        }

        // 分析赋值表达式
        if (assignment->value)
        {
            analyzeExpression(assignment->value.get(), stmt->tableName);

            // 检查类型兼容性
            DataType columnType = catalog_->getColumnType(stmt->tableName, assignment->columnName);
            DataType valueType = inferExpressionType(assignment->value.get(), stmt->tableName);

            // 基本类型检查（可以根据需要扩展更复杂的类型转换规则）
            if (columnType != valueType)
            {
                // 允许数值类型之间的转换
                bool isValidConversion = false;
                if ((columnType == DataType::INT || columnType == DataType::DOUBLE) &&
                    (valueType == DataType::INT || valueType == DataType::DOUBLE))
                {
                    isValidConversion = true;
                }

                if (!isValidConversion)
                {
                    addWarning("Type mismatch for column '" + assignment->columnName +
                               "': expected " + std::to_string(static_cast<int>(columnType)) +
                               ", got " + std::to_string(static_cast<int>(valueType)));
                }
            }
        }
        else
        {
            addError(SemanticErrorType::INVALID_VALUE,
                     "Assignment value cannot be null for column '" + assignment->columnName + "'");
        }
    }

    // 检查WHERE子句
    if (stmt->whereClause)
    {
        analyzeExpression(stmt->whereClause.get(), stmt->tableName);
    }
}

void SemanticAnalyzer::analyzeCreateIndex(CreateIndexStatement *stmt)
{
    // 检查索引名
    if (stmt->indexName.empty())
    {
        addError(SemanticErrorType::UNKNOWN_ERROR, "Index name cannot be empty");
        return;
    }

    // 检查表是否存在
    if (!catalog_->tableExists(stmt->tableName))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + stmt->tableName + "' does not exist");
        return;
    }

    // 检查列是否存在
    if (!catalog_->columnExists(stmt->tableName, stmt->columnName))
    {
        addError(SemanticErrorType::COLUMN_NOT_EXISTS,
                 "Column '" + stmt->columnName + "' does not exist in table '" + stmt->tableName + "'");
        return;
    }

    // 可以添加更多索引特定的检查，比如索引名是否已存在等
}

void SemanticAnalyzer::analyzeExpression(Expression *expr, const std::string &contextTable)
{
    if (!expr)
        return;

    switch (expr->nodeType)
    {
    case ASTNodeType::IDENTIFIER_EXPR:
        analyzeIdentifier(static_cast<IdentifierExpression *>(expr), contextTable);
        break;

    case ASTNodeType::BINARY_EXPR:
        analyzeBinaryExpression(static_cast<BinaryExpression *>(expr), contextTable);
        break;

    case ASTNodeType::UNARY_EXPR:
    {
        auto *unary = static_cast<UnaryExpression *>(expr);
        analyzeExpression(unary->operand.get(), contextTable);
        break;
    }

    case ASTNodeType::LITERAL_EXPR:
        // 字面量无需检查
        break;

    default:
        break;
    }
}

void SemanticAnalyzer::analyzeBinaryExpression(BinaryExpression *expr, const std::string &contextTable)
{
    if (!expr->left || !expr->right)
    {
        addError(SemanticErrorType::UNKNOWN_ERROR, "Binary expression missing operands");
        return;
    }

    // 分析左右操作数
    analyzeExpression(expr->left.get(), contextTable);
    analyzeExpression(expr->right.get(), contextTable);

    // 类型检查
    DataType leftType = inferExpressionType(expr->left.get(), contextTable);
    DataType rightType = inferExpressionType(expr->right.get(), contextTable);

    switch (expr->operator_)
    {
    case TokenType::PLUS:
    case TokenType::MINUS:
    case TokenType::MULTIPLY:
    case TokenType::DIVIDE:
        if (!isArithmeticCompatible(leftType, rightType))
        {
            addError(SemanticErrorType::TYPE_MISMATCH,
                     "Arithmetic operation not supported between these types");
        }
        break;

    case TokenType::EQUAL:
    case TokenType::NOT_EQUAL:
    case TokenType::LESS_THAN:
    case TokenType::LESS_EQUAL:
    case TokenType::GREATER_THAN:
    case TokenType::GREATER_EQUAL:
        if (!isComparableTypes(leftType, rightType))
        {
            addError(SemanticErrorType::TYPE_MISMATCH,
                     "Comparison not supported between these types");
        }
        break;

    case TokenType::AND:
    case TokenType::OR:
        // 逻辑运算符通常不严格要求布尔类型，但建议检查
        break;

    default:
        addWarning("Unknown binary operator");
        break;
    }
}

void SemanticAnalyzer::analyzeIdentifier(IdentifierExpression *expr, const std::string &contextTable)
{
    std::string tableName = expr->tableName.empty() ? contextTable : expr->tableName;

    if (tableName.empty())
    {
        addError(SemanticErrorType::AMBIGUOUS_COLUMN,
                 "Column '" + expr->name + "' is ambiguous - no table context");
        return;
    }

    // 检查表是否存在
    if (!catalog_->tableExists(tableName))
    {
        addError(SemanticErrorType::TABLE_NOT_EXISTS,
                 "Table '" + tableName + "' does not exist");
        return;
    }

    // 特殊情况：SELECT *
    if (expr->name == "*")
    {
        return; // * 是有效的
    }

    // 检查列是否存在
    if (!catalog_->columnExists(tableName, expr->name))
    {
        addError(SemanticErrorType::COLUMN_NOT_EXISTS,
                 "Column '" + expr->name + "' does not exist in table '" + tableName + "'");
    }
}

void SemanticAnalyzer::analyzeJoinCondition(Expression *expr, const std::string &leftTable, const std::string &rightTable)
{
    if (!expr)
        return;

    switch (expr->nodeType)
    {
    case ASTNodeType::IDENTIFIER_EXPR:
    {
        auto *identExpr = static_cast<IdentifierExpression *>(expr);
        std::string tableName = identExpr->tableName;

        if (tableName.empty())
        {
            // 如果没有指定表名，检查列是否在两个表中都存在（会导致歧义）
            bool inLeftTable = catalog_->columnExists(leftTable, identExpr->name);
            bool inRightTable = catalog_->columnExists(rightTable, identExpr->name);

            if (inLeftTable && inRightTable)
            {
                addError(SemanticErrorType::AMBIGUOUS_COLUMN,
                         "Column '" + identExpr->name + "' is ambiguous - exists in both '" +
                             leftTable + "' and '" + rightTable + "'");
            }
            else if (inLeftTable)
            {
                // 在左表中找到
            }
            else if (inRightTable)
            {
                // 在右表中找到
            }
            else
            {
                addError(SemanticErrorType::COLUMN_NOT_EXISTS,
                         "Column '" + identExpr->name + "' does not exist in either '" +
                             leftTable + "' or '" + rightTable + "'");
            }
        }
        else
        {
            // 指定了表名，检查表名是否正确，列是否存在
            if (tableName != leftTable && tableName != rightTable)
            {
                addError(SemanticErrorType::TABLE_NOT_EXISTS,
                         "Table '" + tableName + "' is not part of this JOIN operation");
            }
            else if (!catalog_->columnExists(tableName, identExpr->name))
            {
                addError(SemanticErrorType::COLUMN_NOT_EXISTS,
                         "Column '" + identExpr->name + "' does not exist in table '" + tableName + "'");
            }
        }
        break;
    }

    case ASTNodeType::BINARY_EXPR:
    {
        auto *binaryExpr = static_cast<BinaryExpression *>(expr);
        analyzeJoinCondition(binaryExpr->left.get(), leftTable, rightTable);
        analyzeJoinCondition(binaryExpr->right.get(), leftTable, rightTable);
        break;
    }

    case ASTNodeType::UNARY_EXPR:
    {
        auto *unaryExpr = static_cast<UnaryExpression *>(expr);
        analyzeJoinCondition(unaryExpr->operand.get(), leftTable, rightTable);
        break;
    }

    case ASTNodeType::LITERAL_EXPR:
        // 字面量无需检查
        break;

    default:
        break;
    }
}

bool SemanticAnalyzer::isNumericType(DataType type) const
{
    return type == DataType::INT || type == DataType::DOUBLE;
}

bool SemanticAnalyzer::isComparableTypes(DataType left, DataType right) const
{
    // 同类型可比较
    if (left == right)
        return true;

    // 数值类型之间可比较
    if (isNumericType(left) && isNumericType(right))
        return true;

    return false;
}

bool SemanticAnalyzer::isArithmeticCompatible(DataType left, DataType right) const
{
    return isNumericType(left) && isNumericType(right);
}

DataType SemanticAnalyzer::getResultType(DataType left, DataType right, TokenType op) const
{
    switch (op)
    {
    case TokenType::PLUS:
    case TokenType::MINUS:
    case TokenType::MULTIPLY:
    case TokenType::DIVIDE:
        if (left == DataType::DOUBLE || right == DataType::DOUBLE)
        {
            return DataType::DOUBLE;
        }
        return DataType::INT;

    case TokenType::EQUAL:
    case TokenType::NOT_EQUAL:
    case TokenType::LESS_THAN:
    case TokenType::LESS_EQUAL:
    case TokenType::GREATER_THAN:
    case TokenType::GREATER_EQUAL:
    case TokenType::AND:
    case TokenType::OR:
        return DataType::INT; // 当作布尔值使用

    default:
        return left; // 默认返回左操作数类型
    }
}

std::string SemanticAnalyzer::semanticErrorTypeToString(SemanticErrorType type) const
{
    switch (type)
    {
    case SemanticErrorType::TABLE_NOT_EXISTS:
        return "TABLE_NOT_EXISTS";
    case SemanticErrorType::TABLE_ALREADY_EXISTS:
        return "TABLE_ALREADY_EXISTS";
    case SemanticErrorType::COLUMN_NOT_EXISTS:
        return "COLUMN_NOT_EXISTS";
    case SemanticErrorType::COLUMN_ALREADY_EXISTS:
        return "COLUMN_ALREADY_EXISTS";
    case SemanticErrorType::TYPE_MISMATCH:
        return "TYPE_MISMATCH";
    case SemanticErrorType::COLUMN_COUNT_MISMATCH:
        return "COLUMN_COUNT_MISMATCH";
    case SemanticErrorType::INVALID_DATA_TYPE:
        return "INVALID_DATA_TYPE";
    case SemanticErrorType::DUPLICATE_COLUMN_NAME:
        return "DUPLICATE_COLUMN_NAME";
    case SemanticErrorType::EMPTY_TABLE_NAME:
        return "EMPTY_TABLE_NAME";
    case SemanticErrorType::EMPTY_COLUMN_NAME:
        return "EMPTY_COLUMN_NAME";
    case SemanticErrorType::INVALID_VALUE:
        return "INVALID_VALUE";
    case SemanticErrorType::MISSING_PRIMARY_KEY:
        return "MISSING_PRIMARY_KEY";
    case SemanticErrorType::DUPLICATE_PRIMARY_KEY:
        return "DUPLICATE_PRIMARY_KEY";
    case SemanticErrorType::AMBIGUOUS_COLUMN:
        return "AMBIGUOUS_COLUMN";
    case SemanticErrorType::INVALID_FUNCTION:
        return "INVALID_FUNCTION";
    case SemanticErrorType::UNKNOWN_ERROR:
        return "UNKNOWN_ERROR";
    default:
        return "UNKNOWN";
    }
}
