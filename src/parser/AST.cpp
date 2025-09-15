#include "../../include/parser/AST.h"
#include <iostream>
#include <sstream>

// LiteralExpression实现
void LiteralExpression::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string LiteralExpression::toString(int indent) const
{
    std::ostringstream oss;
    oss << getIndent(indent) << "LiteralExpression: ";

    std::visit([&oss](const auto &val)
               {
        using T = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<T, int>) {
            oss << val;
        } else if constexpr (std::is_same_v<T, double>) {
            oss << val;
        } else if constexpr (std::is_same_v<T, std::string>) {
            oss << "\"" << val << "\"";
        } }, value);

    return oss.str();
}

// IdentifierExpression实现
void IdentifierExpression::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string IdentifierExpression::toString(int indent) const
{
    std::string result = getIndent(indent) + "IdentifierExpression: ";
    if (!tableName.empty())
    {
        result += tableName + ".";
    }
    result += name;
    return result;
}

// BinaryExpression实现
void BinaryExpression::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string BinaryExpression::toString(int indent) const
{
    std::string result = getIndent(indent) + "BinaryExpression: " + tokenTypeToString(operator_) + "\n";
    result += left->toString(indent + 1) + "\n";
    result += right->toString(indent + 1);
    return result;
}

// UnaryExpression实现
void UnaryExpression::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string UnaryExpression::toString(int indent) const
{
    std::string result = getIndent(indent) + "UnaryExpression: " + tokenTypeToString(operator_) + "\n";
    result += operand->toString(indent + 1);
    return result;
}

// AggregateExpression实现
void AggregateExpression::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string AggregateExpression::toString(int indent) const
{
    std::string result = getIndent(indent) + "AggregateExpression: " + tokenTypeToString(function) + "\n";
    if (argument)
    {
        result += argument->toString(indent + 1);
    }
    return result;
}

// JoinClause实现
void JoinClause::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string JoinClause::toString(int indent) const
{
    std::string result = getIndent(indent) + "JoinClause: ";

    // 添加JOIN类型
    switch (joinType)
    {
    case JoinType::INNER:
        result += "INNER";
        break;
    case JoinType::LEFT:
        result += "LEFT";
        break;
    case JoinType::RIGHT:
        result += "RIGHT";
        break;
    case JoinType::FULL_OUTER:
        result += "FULL OUTER";
        break;
    }
    result += " JOIN " + rightTable + "\n";

    // 添加ON条件
    if (onCondition)
    {
        result += getIndent(indent + 1) + "ON:\n";
        result += onCondition->toString(indent + 2);
    }

    return result;
}

// ColumnDefinition实现
void ColumnDefinition::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string ColumnDefinition::toString(int indent) const
{
    std::string result = getIndent(indent) + "ColumnDefinition: " + columnName;

    switch (dataType)
    {
    case DataType::INT:
        result += " INT";
        break;
    case DataType::DOUBLE:
        result += " DOUBLE";
        break;
    case DataType::STRING:
        result += " STRING";
        break;
    }

    if (isNotNull)
        result += " NOT NULL";
    if (isPrimaryKey)
        result += " PRIMARY KEY";

    return result;
}

// CreateTableStatement实现
void CreateTableStatement::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string CreateTableStatement::toString(int indent) const
{
    std::string result = getIndent(indent) + "CreateTableStatement: " + tableName + "\n";
    for (const auto &column : columns)
    {
        result += column->toString(indent + 1) + "\n";
    }
    return result;
}

// InsertStatement实现
void InsertStatement::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string InsertStatement::toString(int indent) const
{
    std::string result = getIndent(indent) + "InsertStatement: " + tableName + "\n";

    if (!columns.empty())
    {
        result += getIndent(indent + 1) + "Columns: ";
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (i > 0)
                result += ", ";
            result += columns[i];
        }
        result += "\n";
    }

    result += getIndent(indent + 1) + "Values:\n";
    for (size_t i = 0; i < valuesList.size(); ++i)
    {
        result += getIndent(indent + 2) + "Row " + std::to_string(i) + ":\n";
        for (const auto &expr : valuesList[i])
        {
            result += expr->toString(indent + 3) + "\n";
        }
    }

    return result;
}

// SelectStatement实现
void SelectStatement::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string SelectStatement::toString(int indent) const
{
    std::string result = getIndent(indent) + "SelectStatement:\n";

    result += getIndent(indent + 1) + "SELECT:\n";
    for (const auto &expr : selectList)
    {
        result += expr->toString(indent + 2) + "\n";
    }

    result += getIndent(indent + 1) + "FROM: " + fromTable + "\n";

    if (!joinClauses.empty())
    {
        for (const auto &joinClause : joinClauses)
        {
            result += joinClause->toString(indent + 1) + "\n";
        }
    }

    if (whereClause)
    {
        result += getIndent(indent + 1) + "WHERE:\n";
        result += whereClause->toString(indent + 2) + "\n";
    }

    if (!groupByList.empty())
    {
        result += getIndent(indent + 1) + "GROUP BY:\n";
        for (const auto &expr : groupByList)
        {
            result += expr->toString(indent + 2) + "\n";
        }
    }

    if (!orderByList.empty())
    {
        result += getIndent(indent + 1) + "ORDER BY:\n";
        for (const auto &item : orderByList)
        {
            result += item->expression->toString(indent + 2);
            result += " " + std::string(item->ascending ? "ASC" : "DESC") + "\n";
        }
    }

    return result;
}

// CreateIndexStatement实现
void CreateIndexStatement::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string CreateIndexStatement::toString(int indent) const
{
    std::string result = getIndent(indent) + "CreateIndexStatement:\n";
    result += getIndent(indent + 1) + "Index: " + indexName + "\n";
    result += getIndent(indent + 1) + "Table: " + tableName + "\n";
    result += getIndent(indent + 1) + "Column: " + columnName + "\n";
    result += getIndent(indent + 1) + "Unique: " + (isUnique ? "YES" : "NO") + "\n";
    return result;
}

// DeleteStatement实现
void DeleteStatement::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string DeleteStatement::toString(int indent) const
{
    std::string result = getIndent(indent) + "DeleteStatement: " + tableName + "\n";

    if (whereClause)
    {
        result += getIndent(indent + 1) + "WHERE:\n";
        result += whereClause->toString(indent + 2) + "\n";
    }

    return result;
}

// UpdateStatement实现
void UpdateStatement::accept(ASTVisitor *visitor)
{
    visitor->visit(this);
}

std::string UpdateStatement::toString(int indent) const
{
    std::string result = getIndent(indent) + "UpdateStatement: " + tableName + "\n";

    if (!assignments.empty())
    {
        result += getIndent(indent + 1) + "SET:\n";
        for (const auto &assignment : assignments)
        {
            result += getIndent(indent + 2) + assignment->columnName + " = ";
            if (assignment->value)
            {
                result += assignment->value->toString(0) + "\n";
            }
            else
            {
                result += "NULL\n";
            }
        }
    }

    if (whereClause)
    {
        result += getIndent(indent + 1) + "WHERE:\n";
        result += whereClause->toString(indent + 2) + "\n";
    }

    return result;
}

// ASTPrinter实现
void ASTPrinter::visit(LiteralExpression *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(IdentifierExpression *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(BinaryExpression *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(UnaryExpression *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(AggregateExpression *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(JoinClause *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(ColumnDefinition *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(CreateTableStatement *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(CreateIndexStatement *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(InsertStatement *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(SelectStatement *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(DeleteStatement *node)
{
    std::cout << node->toString();
}

void ASTPrinter::visit(UpdateStatement *node)
{
    std::cout << node->toString();
}
