#include "../../include/parser/Parser.h"
#include <iostream>

Parser::Parser(const std::string &input) : lexer_(input)
{
    advance();
}

std::unique_ptr<Statement> Parser::parseStatement()
{
    try
    {
        if (match(TokenType::CREATE))
        {
            return parseCreateStatement();
        }
        else if (match(TokenType::INSERT))
        {
            return parseInsertStatement();
        }
        else if (match(TokenType::SELECT))
        {
            return parseSelectStatement();
        }
        else if (match(TokenType::DELETE))
        {
            return parseDeleteStatement();
        }
        else if (match(TokenType::UPDATE))
        {
            return parseUpdateStatement();
        }
        else
        {
            addError("Expected statement (CREATE, INSERT, SELECT, DELETE, UPDATE)");
            synchronize();
            return nullptr;
        }
    }
    catch (const std::exception &e)
    {
        addError("Unexpected error: " + std::string(e.what()));
        synchronize();
        return nullptr;
    }
}

std::vector<std::unique_ptr<Statement>> Parser::parseStatements()
{
    std::vector<std::unique_ptr<Statement>> statements;

    while (!isAtEnd())
    {
        auto stmt = parseStatement();
        if (stmt)
        {
            statements.push_back(std::move(stmt));
        }

        if (match(TokenType::SEMICOLON))
        {
            // 分号已被消费
        }

        if (isAtEnd())
            break;
    }

    return statements;
}

bool Parser::hasErrors() const
{
    return !errors_.empty();
}

const std::vector<ParseError> &Parser::getErrors() const
{
    return errors_;
}

void Parser::printErrors() const
{
    for (const auto &error : errors_)
    {
        std::cerr << error.toString() << std::endl;
    }
}

void Parser::reset()
{
    lexer_.reset();
    errors_.clear();
    advance();
}

void Parser::advance()
{
    currentToken_ = lexer_.nextToken();
}

bool Parser::match(TokenType type)
{
    if (check(type))
    {
        advance();
        return true;
    }
    return false;
}

bool Parser::check(TokenType type) const
{
    return currentToken_.type == type;
}

Token Parser::consume(TokenType type, const std::string &message)
{
    if (check(type))
    {
        Token token = currentToken_;
        advance();
        return token;
    }

    addError(message);
    return Token(TokenType::ERROR, "", currentToken_.line, currentToken_.column);
}

void Parser::addError(const std::string &message)
{
    errors_.emplace_back(message, currentToken_.line, currentToken_.column);
}

void Parser::synchronize()
{
    advance();

    while (!isAtEnd())
    {
        if (currentToken_.type == TokenType::SEMICOLON)
        {
            advance();
            return;
        }

        switch (currentToken_.type)
        {
        case TokenType::CREATE:
        case TokenType::INSERT:
        case TokenType::SELECT:
        case TokenType::DELETE:
            return;
        default:
            break;
        }

        advance();
    }
}

std::unique_ptr<Statement> Parser::parseCreateStatement()
{
    if (check(TokenType::TABLE))
    {
        return parseCreateTableStatement();
    }
    else if (check(TokenType::UNIQUE))
    {
        advance(); // consume UNIQUE
        consume(TokenType::INDEX, "Expected 'INDEX' after 'UNIQUE'");
        return parseCreateIndexStatement(true);
    }
    else if (check(TokenType::INDEX))
    {
        return parseCreateIndexStatement(false);
    }
    else
    {
        addError("Expected 'TABLE' or 'INDEX' after 'CREATE'");
        return nullptr;
    }
}

std::unique_ptr<Statement> Parser::parseCreateTableStatement()
{
    consume(TokenType::TABLE, "Expected 'TABLE' after 'CREATE'");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    if (tableName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    auto stmt = std::make_unique<CreateTableStatement>(tableName.value);

    consume(TokenType::LEFT_PAREN, "Expected '(' after table name");
    stmt->columns = parseColumnDefinitions();
    consume(TokenType::RIGHT_PAREN, "Expected ')' after column definitions");

    return std::move(stmt);
}

std::unique_ptr<Statement> Parser::parseCreateIndexStatement(bool isUnique)
{
    consume(TokenType::INDEX, "Expected 'INDEX'");

    Token indexName = consume(TokenType::IDENTIFIER, "Expected index name");
    if (indexName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    consume(TokenType::ON, "Expected 'ON' after index name");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    if (tableName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    consume(TokenType::LEFT_PAREN, "Expected '(' after table name");

    Token columnName = consume(TokenType::IDENTIFIER, "Expected column name");
    if (columnName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    consume(TokenType::RIGHT_PAREN, "Expected ')' after column name");

    return std::make_unique<CreateIndexStatement>(indexName.value, tableName.value, columnName.value, isUnique);
}

std::unique_ptr<Statement> Parser::parseInsertStatement()
{
    consume(TokenType::INTO, "Expected 'INTO' after 'INSERT'");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    if (tableName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    auto stmt = std::make_unique<InsertStatement>(tableName.value);

    if (match(TokenType::LEFT_PAREN))
    {
        stmt->columns = parseColumnList();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after column list");
    }

    consume(TokenType::VALUES, "Expected 'VALUES'");
    stmt->valuesList = parseValuesList();

    return std::move(stmt);
}

std::unique_ptr<Statement> Parser::parseSelectStatement()
{
    auto stmt = std::make_unique<SelectStatement>();

    stmt->selectList = parseExpressionList();

    consume(TokenType::FROM, "Expected 'FROM' after SELECT list");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name after 'FROM'");
    if (tableName.type != TokenType::ERROR)
    {
        stmt->fromTable = tableName.value;
    }

    // 解析JOIN子句
    stmt->joinClauses = parseJoinClauses();

    if (match(TokenType::WHERE))
    {
        stmt->whereClause = parseExpression();
    }

    // 解析GROUP BY子句
    if (match(TokenType::GROUP))
    {
        consume(TokenType::BY, "Expected 'BY' after 'GROUP'");
        stmt->groupByList = parseExpressionList();
    }

    // 解析ORDER BY子句
    if (match(TokenType::ORDER))
    {
        consume(TokenType::BY, "Expected 'BY' after 'ORDER'");
        stmt->orderByList = parseOrderByList();
    }

    // 检查解析过程中是否有错误
    if (hasErrors())
    {
        return nullptr;
    }

    return std::move(stmt);
}

std::unique_ptr<Statement> Parser::parseDeleteStatement()
{
    consume(TokenType::FROM, "Expected 'FROM' after 'DELETE'");

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name");
    if (tableName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    auto stmt = std::make_unique<DeleteStatement>(tableName.value);

    if (match(TokenType::WHERE))
    {
        stmt->whereClause = parseExpression();
    }

    return std::move(stmt);
}

std::unique_ptr<Statement> Parser::parseUpdateStatement()
{
    // UPDATE table_name SET column1=value1, column2=value2 WHERE condition

    Token tableName = consume(TokenType::IDENTIFIER, "Expected table name after 'UPDATE'");
    if (tableName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    auto stmt = std::make_unique<UpdateStatement>(tableName.value);

    consume(TokenType::SET, "Expected 'SET' after table name");

    // 解析赋值列表：column1=value1, column2=value2, ...
    do
    {
        Token columnName = consume(TokenType::IDENTIFIER, "Expected column name");
        if (columnName.type == TokenType::ERROR)
        {
            return nullptr;
        }

        consume(TokenType::EQUAL, "Expected '=' after column name");

        auto value = parseExpression();
        if (!value)
        {
            addError("Expected value expression after '='");
            return nullptr;
        }

        stmt->assignments.push_back(
            std::make_unique<UpdateAssignment>(columnName.value, std::move(value)));

    } while (match(TokenType::COMMA));

    // 可选的WHERE子句
    if (match(TokenType::WHERE))
    {
        stmt->whereClause = parseExpression();
    }

    return std::move(stmt);
}

std::unique_ptr<Expression> Parser::parseExpression()
{
    return parseLogicalOr();
}

std::unique_ptr<Expression> Parser::parseLogicalOr()
{
    auto expr = parseLogicalAnd();

    while (match(TokenType::OR))
    {
        TokenType operator_ = TokenType::OR;
        auto right = parseLogicalAnd();
        expr = std::make_unique<BinaryExpression>(std::move(expr), operator_, std::move(right));
    }

    return expr;
}

std::unique_ptr<Expression> Parser::parseLogicalAnd()
{
    auto expr = parseEquality();

    while (match(TokenType::AND))
    {
        TokenType operator_ = TokenType::AND;
        auto right = parseEquality();
        expr = std::make_unique<BinaryExpression>(std::move(expr), operator_, std::move(right));
    }

    return expr;
}

std::unique_ptr<Expression> Parser::parseEquality()
{
    auto expr = parseComparison();

    while (currentToken_.type == TokenType::EQUAL || currentToken_.type == TokenType::NOT_EQUAL)
    {
        TokenType operator_ = currentToken_.type;
        advance();
        auto right = parseComparison();
        expr = std::make_unique<BinaryExpression>(std::move(expr), operator_, std::move(right));
    }

    return expr;
}

std::unique_ptr<Expression> Parser::parseComparison()
{
    auto expr = parseTerm();

    while (currentToken_.type == TokenType::GREATER_THAN || currentToken_.type == TokenType::GREATER_EQUAL ||
           currentToken_.type == TokenType::LESS_THAN || currentToken_.type == TokenType::LESS_EQUAL)
    {
        TokenType operator_ = currentToken_.type;
        advance();
        auto right = parseTerm();
        expr = std::make_unique<BinaryExpression>(std::move(expr), operator_, std::move(right));
    }

    return expr;
}

std::unique_ptr<Expression> Parser::parseTerm()
{
    auto expr = parseFactor();

    while (currentToken_.type == TokenType::PLUS || currentToken_.type == TokenType::MINUS)
    {
        TokenType operator_ = currentToken_.type;
        advance();
        auto right = parseFactor();
        expr = std::make_unique<BinaryExpression>(std::move(expr), operator_, std::move(right));
    }

    return expr;
}

std::unique_ptr<Expression> Parser::parseFactor()
{
    auto expr = parseUnary();

    while (currentToken_.type == TokenType::MULTIPLY || currentToken_.type == TokenType::DIVIDE)
    {
        TokenType operator_ = currentToken_.type;
        advance();
        auto right = parseUnary();
        expr = std::make_unique<BinaryExpression>(std::move(expr), operator_, std::move(right));
    }

    return expr;
}

std::unique_ptr<Expression> Parser::parseUnary()
{
    if (currentToken_.type == TokenType::NOT || currentToken_.type == TokenType::MINUS)
    {
        TokenType operator_ = currentToken_.type;
        advance();
        auto expr = parseUnary();
        return std::make_unique<UnaryExpression>(operator_, std::move(expr));
    }

    return parsePrimary();
}

std::unique_ptr<Expression> Parser::parsePrimary()
{
    if (currentToken_.type == TokenType::INTEGER)
    {
        int value = std::stoi(currentToken_.value);
        advance();
        return std::make_unique<LiteralExpression>(Value(value));
    }

    if (currentToken_.type == TokenType::FLOAT)
    {
        double value = std::stod(currentToken_.value);
        advance();
        return std::make_unique<LiteralExpression>(Value(value));
    }

    if (currentToken_.type == TokenType::STRING_LITERAL)
    {
        std::string value = currentToken_.value;
        advance();
        return std::make_unique<LiteralExpression>(Value(value));
    }

    if (match(TokenType::TRUE_TOKEN))
    {
        return std::make_unique<LiteralExpression>(Value(1));
    }

    if (match(TokenType::FALSE_TOKEN))
    {
        return std::make_unique<LiteralExpression>(Value(0));
    }

    if (match(TokenType::NULL_TOKEN))
    {
        return std::make_unique<LiteralExpression>(Value(std::string("NULL")));
    }

    // 检查聚合函数
    if (currentToken_.type == TokenType::COUNT || currentToken_.type == TokenType::SUM ||
        currentToken_.type == TokenType::AVG || currentToken_.type == TokenType::MAX ||
        currentToken_.type == TokenType::MIN)
    {
        TokenType funcType = currentToken_.type;
        advance();

        consume(TokenType::LEFT_PAREN, "Expected '(' after aggregate function");
        auto argument = parseExpression();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after aggregate function argument");

        return std::make_unique<AggregateExpression>(funcType, std::move(argument));
    }

    if (currentToken_.type == TokenType::IDENTIFIER)
    {
        std::string name = currentToken_.value;
        advance();

        if (match(TokenType::DOT))
        {
            Token columnName = consume(TokenType::IDENTIFIER, "Expected column name after '.'");
            if (columnName.type != TokenType::ERROR)
            {
                return std::make_unique<IdentifierExpression>(columnName.value, name);
            }
        }

        return std::make_unique<IdentifierExpression>(name);
    }

    if (match(TokenType::MULTIPLY))
    {
        return std::make_unique<IdentifierExpression>("*");
    }

    if (match(TokenType::LEFT_PAREN))
    {
        auto expr = parseExpression();
        consume(TokenType::RIGHT_PAREN, "Expected ')' after expression");
        return expr;
    }

    addError("Expected expression");
    return nullptr;
}

std::vector<std::unique_ptr<ColumnDefinition>> Parser::parseColumnDefinitions()
{
    std::vector<std::unique_ptr<ColumnDefinition>> columns;

    if (!check(TokenType::RIGHT_PAREN))
    {
        do
        {
            auto column = parseColumnDefinition();
            if (column)
            {
                columns.push_back(std::move(column));
            }
        } while (match(TokenType::COMMA));
    }

    return columns;
}

std::unique_ptr<ColumnDefinition> Parser::parseColumnDefinition()
{
    Token columnName = consume(TokenType::IDENTIFIER, "Expected column name");
    if (columnName.type == TokenType::ERROR)
    {
        return nullptr;
    }

    DataType dataType = parseDataType();

    bool isNotNull = false;
    bool isPrimaryKey = false;

    while (check(TokenType::NOT) || check(TokenType::PRIMARY))
    {
        if (match(TokenType::NOT))
        {
            if (match(TokenType::NULL_TOKEN))
            {
                isNotNull = true;
            }
            else
            {
                addError("Expected 'NULL' after 'NOT'");
            }
        }
        else if (match(TokenType::PRIMARY))
        {
            if (match(TokenType::KEY))
            {
                isPrimaryKey = true;
            }
            else
            {
                addError("Expected 'KEY' after 'PRIMARY'");
            }
        }
        else
        {
            break;
        }
    }

    return std::make_unique<ColumnDefinition>(columnName.value, dataType, isNotNull, isPrimaryKey);
}

DataType Parser::parseDataType()
{
    if (match(TokenType::INT))
    {
        return DataType::INT;
    }
    else if (match(TokenType::DOUBLE))
    {
        return DataType::DOUBLE;
    }
    else if (match(TokenType::STRING))
    {
        return DataType::STRING;
    }
    else
    {
        addError("Expected data type (INT, DOUBLE, STRING)");
        return DataType::STRING;
    }
}

std::vector<std::string> Parser::parseColumnList()
{
    std::vector<std::string> columns;

    if (!check(TokenType::RIGHT_PAREN))
    {
        do
        {
            Token columnName = consume(TokenType::IDENTIFIER, "Expected column name");
            if (columnName.type != TokenType::ERROR)
            {
                columns.push_back(columnName.value);
            }
        } while (match(TokenType::COMMA));
    }

    return columns;
}

std::vector<std::unique_ptr<Expression>> Parser::parseExpressionList()
{
    std::vector<std::unique_ptr<Expression>> expressions;

    if (!check(TokenType::FROM) && !check(TokenType::SEMICOLON) && !isAtEnd())
    {
        do
        {
            auto expr = parseExpression();
            if (expr)
            {
                expressions.push_back(std::move(expr));
            }
        } while (match(TokenType::COMMA));
    }

    return expressions;
}

std::vector<std::vector<std::unique_ptr<Expression>>> Parser::parseValuesList()
{
    std::vector<std::vector<std::unique_ptr<Expression>>> valuesList;

    do
    {
        consume(TokenType::LEFT_PAREN, "Expected '(' before values");

        std::vector<std::unique_ptr<Expression>> values;
        if (!check(TokenType::RIGHT_PAREN))
        {
            do
            {
                auto expr = parseExpression();
                if (expr)
                {
                    values.push_back(std::move(expr));
                }
            } while (match(TokenType::COMMA));
        }

        consume(TokenType::RIGHT_PAREN, "Expected ')' after values");
        valuesList.push_back(std::move(values));

    } while (match(TokenType::COMMA));

    return valuesList;
}

bool Parser::isAtEnd() const
{
    return currentToken_.type == TokenType::END_OF_FILE;
}

bool Parser::isDataType(TokenType type) const
{
    return type == TokenType::INT || type == TokenType::DOUBLE || type == TokenType::STRING;
}

bool Parser::isBinaryOperator(TokenType type) const
{
    return type == TokenType::PLUS || type == TokenType::MINUS ||
           type == TokenType::MULTIPLY || type == TokenType::DIVIDE ||
           type == TokenType::EQUAL || type == TokenType::NOT_EQUAL ||
           type == TokenType::LESS_THAN || type == TokenType::LESS_EQUAL ||
           type == TokenType::GREATER_THAN || type == TokenType::GREATER_EQUAL ||
           type == TokenType::AND || type == TokenType::OR;
}

bool Parser::isUnaryOperator(TokenType type) const
{
    return type == TokenType::NOT || type == TokenType::MINUS;
}

std::vector<std::unique_ptr<OrderByItem>> Parser::parseOrderByList()
{
    std::vector<std::unique_ptr<OrderByItem>> orderByList;

    do
    {
        auto expr = parseExpression();
        if (!expr)
        {
            break;
        }

        bool ascending = true; // 默认为ASC
        if (match(TokenType::ASC))
        {
            ascending = true;
        }
        else if (match(TokenType::DESC))
        {
            ascending = false;
        }

        auto orderItem = std::make_unique<OrderByItem>(std::move(expr), ascending);
        orderByList.push_back(std::move(orderItem));

    } while (match(TokenType::COMMA));

    return orderByList;
}

std::vector<std::unique_ptr<JoinClause>> Parser::parseJoinClauses()
{
    std::vector<std::unique_ptr<JoinClause>> joinClauses;

    while (check(TokenType::INNER) || check(TokenType::LEFT) || check(TokenType::RIGHT) ||
           check(TokenType::FULL) || check(TokenType::JOIN))
    {
        auto joinClause = parseJoinClause();
        if (joinClause)
        {
            joinClauses.push_back(std::move(joinClause));
        }
        else
        {
            break; // 解析错误，停止
        }
    }

    return joinClauses;
}

std::unique_ptr<JoinClause> Parser::parseJoinClause()
{
    JoinType joinType = parseJoinType();

    // 确保后面跟着JOIN关键字（如果还没有消费JOIN token）
    if (!check(TokenType::JOIN))
    {
        consume(TokenType::JOIN, "Expected 'JOIN' after join type");
    }
    else
    {
        advance(); // 消费JOIN token
    }

    // 解析右表名
    Token rightTableToken = consume(TokenType::IDENTIFIER, "Expected table name after JOIN");
    if (rightTableToken.type == TokenType::ERROR)
    {
        return nullptr;
    }

    // 解析ON条件
    Token onToken = consume(TokenType::ON, "Expected 'ON' after table name in JOIN");
    if (onToken.type == TokenType::ERROR)
    {
        return nullptr;
    }

    auto onCondition = parseExpression();
    if (!onCondition)
    {
        addError("Expected condition after 'ON'");
        return nullptr;
    }

    return std::make_unique<JoinClause>(joinType, rightTableToken.value, std::move(onCondition));
}

JoinType Parser::parseJoinType()
{
    if (match(TokenType::INNER))
    {
        return JoinType::INNER;
    }
    else if (match(TokenType::LEFT))
    {
        // 可选的OUTER关键字
        if (check(TokenType::OUTER))
        {
            advance();
        }
        return JoinType::LEFT;
    }
    else if (match(TokenType::RIGHT))
    {
        // 可选的OUTER关键字
        if (check(TokenType::OUTER))
        {
            advance();
        }
        return JoinType::RIGHT;
    }
    else if (match(TokenType::FULL))
    {
        // 可选的OUTER关键字
        if (check(TokenType::OUTER))
        {
            advance();
        }
        return JoinType::FULL_OUTER;
    }
    else if (check(TokenType::JOIN))
    {
        // 默认为INNER JOIN，不消费JOIN token（会在parseJoinClause中消费）
        return JoinType::INNER;
    }
    else
    {
        addError("Expected JOIN type (INNER, LEFT, RIGHT, FULL) or JOIN");
        return JoinType::INNER;
    }
}
