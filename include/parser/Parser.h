#pragma once
#include "Lexer.h"
#include "AST.h"
#include <memory>
#include <vector>

// 语法分析错误类
class ParseError
{
public:
    std::string message;
    size_t line;
    size_t column;

    ParseError(const std::string &msg, size_t l, size_t c)
        : message(msg), line(l), column(c) {}

    std::string toString() const
    {
        return "Parse Error at " + std::to_string(line) + ":" + std::to_string(column) + " - " + message;
    }
};

// 语法分析器类
class Parser
{
public:
    explicit Parser(const std::string &input);
    ~Parser() = default;

    // 解析SQL语句
    std::unique_ptr<Statement> parseStatement();

    // 解析多个语句
    std::vector<std::unique_ptr<Statement>> parseStatements();

    // 检查是否有错误
    bool hasErrors() const;

    // 获取错误列表
    const std::vector<ParseError> &getErrors() const;

    // 打印所有错误
    void printErrors() const;

    // 重置解析器
    void reset();

private:
    Lexer lexer_;
    Token currentToken_;
    std::vector<ParseError> errors_;

    // Token管理
    void advance();
    bool match(TokenType type);
    bool check(TokenType type) const;
    Token consume(TokenType type, const std::string &message);

    // 错误处理
    void addError(const std::string &message);
    void synchronize(); // 错误恢复

    // 语句解析方法
    std::unique_ptr<Statement> parseCreateStatement();
    std::unique_ptr<Statement> parseCreateTableStatement();
    std::unique_ptr<Statement> parseDropStatement();
    std::unique_ptr<Statement> parseDropTableStatement();
    std::unique_ptr<Statement> parseCreateIndexStatement(bool isUnique);
    std::unique_ptr<Statement> parseInsertStatement();
    std::unique_ptr<Statement> parseSelectStatement();
    std::unique_ptr<Statement> parseDeleteStatement();
    std::unique_ptr<Statement> parseUpdateStatement();

    // 表达式解析方法（递归下降）
    std::unique_ptr<Expression> parseExpression();
    std::unique_ptr<Expression> parseLogicalOr();
    std::unique_ptr<Expression> parseLogicalAnd();
    std::unique_ptr<Expression> parseEquality();
    std::unique_ptr<Expression> parseComparison();
    std::unique_ptr<Expression> parseTerm();
    std::unique_ptr<Expression> parseFactor();
    std::unique_ptr<Expression> parseUnary();
    std::unique_ptr<Expression> parsePrimary();

    // 辅助解析方法
    std::vector<std::unique_ptr<ColumnDefinition>> parseColumnDefinitions();
    std::unique_ptr<ColumnDefinition> parseColumnDefinition();
    DataType parseDataType();
    std::vector<std::string> parseColumnList();
    std::vector<std::unique_ptr<Expression>> parseExpressionList();
    std::vector<std::vector<std::unique_ptr<Expression>>> parseValuesList();
    std::vector<std::unique_ptr<OrderByItem>> parseOrderByList();
    std::vector<std::unique_ptr<JoinClause>> parseJoinClauses();
    std::unique_ptr<JoinClause> parseJoinClause();
    JoinType parseJoinType();

    // 工具方法
    bool isAtEnd() const;
    bool isDataType(TokenType type) const;
    bool isBinaryOperator(TokenType type) const;
    bool isUnaryOperator(TokenType type) const;
};
