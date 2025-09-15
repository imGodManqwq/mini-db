#pragma once
#include "Token.h"
#include "../storage/Row.h"
#include <memory>
#include <vector>
#include <string>

// 前向声明
class ASTVisitor;

// JOIN类型枚举
enum class JoinType
{
    INNER,
    LEFT,
    RIGHT,
    FULL_OUTER
};

// AST节点类型枚举
enum class ASTNodeType
{
    // 语句类型
    CREATE_TABLE_STMT,
    CREATE_INDEX_STMT,
    INSERT_STMT,
    SELECT_STMT,
    DELETE_STMT,
    UPDATE_STMT,

    // 表达式类型
    BINARY_EXPR,
    UNARY_EXPR,
    LITERAL_EXPR,
    IDENTIFIER_EXPR,
    AGGREGATE_EXPR,

    // 其他类型
    COLUMN_DEF,
    TABLE_REF,
    WHERE_CLAUSE,
    VALUE_LIST,
    COLUMN_LIST,
    JOIN_CLAUSE
};

// AST基类
class ASTNode
{
public:
    ASTNodeType nodeType;

    explicit ASTNode(ASTNodeType type) : nodeType(type) {}
    virtual ~ASTNode() = default;

    // 访问者模式
    virtual void accept(ASTVisitor *visitor) = 0;

    // 转换为字符串表示
    virtual std::string toString(int indent = 0) const = 0;

protected:
    std::string getIndent(int level) const
    {
        return std::string(level * 2, ' ');
    }
};

// 表达式基类
class Expression : public ASTNode
{
public:
    explicit Expression(ASTNodeType type) : ASTNode(type) {}
};

// 字面量表达式
class LiteralExpression : public Expression
{
public:
    Value value;

    explicit LiteralExpression(const Value &val)
        : Expression(ASTNodeType::LITERAL_EXPR), value(val) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// 标识符表达式
class IdentifierExpression : public Expression
{
public:
    std::string name;
    std::string tableName; // 可选的表名前缀

    explicit IdentifierExpression(const std::string &n, const std::string &table = "")
        : Expression(ASTNodeType::IDENTIFIER_EXPR), name(n), tableName(table) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// 二元表达式
class BinaryExpression : public Expression
{
public:
    std::unique_ptr<Expression> left;
    TokenType operator_;
    std::unique_ptr<Expression> right;

    BinaryExpression(std::unique_ptr<Expression> l, TokenType op, std::unique_ptr<Expression> r)
        : Expression(ASTNodeType::BINARY_EXPR), left(std::move(l)), operator_(op), right(std::move(r)) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// 一元表达式
class UnaryExpression : public Expression
{
public:
    TokenType operator_;
    std::unique_ptr<Expression> operand;

    UnaryExpression(TokenType op, std::unique_ptr<Expression> expr)
        : Expression(ASTNodeType::UNARY_EXPR), operator_(op), operand(std::move(expr)) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// 列定义
class ColumnDefinition : public ASTNode
{
public:
    std::string columnName;
    DataType dataType;
    bool isNotNull;
    bool isPrimaryKey;

    ColumnDefinition(const std::string &name, DataType type, bool notNull = false, bool primaryKey = false)
        : ASTNode(ASTNodeType::COLUMN_DEF), columnName(name), dataType(type),
          isNotNull(notNull), isPrimaryKey(primaryKey) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// 语句基类
class Statement : public ASTNode
{
public:
    explicit Statement(ASTNodeType type) : ASTNode(type) {}
};

// CREATE TABLE语句
class CreateTableStatement : public Statement
{
public:
    std::string tableName;
    std::vector<std::unique_ptr<ColumnDefinition>> columns;

    explicit CreateTableStatement(const std::string &name)
        : Statement(ASTNodeType::CREATE_TABLE_STMT), tableName(name) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// CREATE INDEX语句
class CreateIndexStatement : public Statement
{
public:
    std::string indexName;
    std::string tableName;
    std::string columnName;
    bool isUnique;

    CreateIndexStatement(const std::string &index, const std::string &table,
                         const std::string &column, bool unique = false)
        : Statement(ASTNodeType::CREATE_INDEX_STMT), indexName(index),
          tableName(table), columnName(column), isUnique(unique) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// INSERT语句
class InsertStatement : public Statement
{
public:
    std::string tableName;
    std::vector<std::string> columns; // 可选的列名列表
    std::vector<std::vector<std::unique_ptr<Expression>>> valuesList;

    explicit InsertStatement(const std::string &name)
        : Statement(ASTNodeType::INSERT_STMT), tableName(name) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// SELECT语句
// ORDER BY子句项
struct OrderByItem
{
    std::unique_ptr<Expression> expression; // 排序表达式
    bool ascending;                         // true为ASC，false为DESC

    OrderByItem(std::unique_ptr<Expression> expr, bool asc = true)
        : expression(std::move(expr)), ascending(asc) {}
};

// 聚合函数表达式
class AggregateExpression : public Expression
{
public:
    TokenType function;                   // COUNT, SUM, AVG, MAX, MIN
    std::unique_ptr<Expression> argument; // 聚合函数的参数

    AggregateExpression(TokenType func, std::unique_ptr<Expression> arg)
        : Expression(ASTNodeType::AGGREGATE_EXPR), function(func), argument(std::move(arg)) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// JOIN子句
class JoinClause : public ASTNode
{
public:
    JoinType joinType;                       // JOIN类型
    std::string rightTable;                  // 右表名
    std::unique_ptr<Expression> onCondition; // ON条件

    JoinClause(JoinType type, const std::string &table, std::unique_ptr<Expression> condition)
        : ASTNode(ASTNodeType::JOIN_CLAUSE), joinType(type), rightTable(table), onCondition(std::move(condition)) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

class SelectStatement : public Statement
{
public:
    std::vector<std::unique_ptr<Expression>> selectList;   // SELECT子句
    std::string fromTable;                                 // FROM子句
    std::vector<std::unique_ptr<JoinClause>> joinClauses;  // JOIN子句（可选）
    std::unique_ptr<Expression> whereClause;               // WHERE子句（可选）
    std::vector<std::unique_ptr<Expression>> groupByList;  // GROUP BY子句（可选）
    std::vector<std::unique_ptr<OrderByItem>> orderByList; // ORDER BY子句（可选）

    SelectStatement() : Statement(ASTNodeType::SELECT_STMT) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// DELETE语句
class DeleteStatement : public Statement
{
public:
    std::string tableName;
    std::unique_ptr<Expression> whereClause; // WHERE子句（可选）

    explicit DeleteStatement(const std::string &name)
        : Statement(ASTNodeType::DELETE_STMT), tableName(name) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// UPDATE语句结构体：用于表示SET子句中的列赋值
struct UpdateAssignment
{
    std::string columnName;
    std::unique_ptr<Expression> value;

    UpdateAssignment(const std::string &column, std::unique_ptr<Expression> val)
        : columnName(column), value(std::move(val)) {}
};

class UpdateStatement : public Statement
{
public:
    std::string tableName;
    std::vector<std::unique_ptr<UpdateAssignment>> assignments; // SET子句
    std::unique_ptr<Expression> whereClause;                    // WHERE子句（可选）

    explicit UpdateStatement(const std::string &name)
        : Statement(ASTNodeType::UPDATE_STMT), tableName(name) {}

    void accept(ASTVisitor *visitor) override;
    std::string toString(int indent = 0) const override;
};

// 访问者模式接口
class ASTVisitor
{
public:
    virtual ~ASTVisitor() = default;

    virtual void visit(LiteralExpression *node) = 0;
    virtual void visit(IdentifierExpression *node) = 0;
    virtual void visit(BinaryExpression *node) = 0;
    virtual void visit(UnaryExpression *node) = 0;
    virtual void visit(AggregateExpression *node) = 0;
    virtual void visit(JoinClause *node) = 0;
    virtual void visit(ColumnDefinition *node) = 0;
    virtual void visit(CreateTableStatement *node) = 0;
    virtual void visit(CreateIndexStatement *node) = 0;
    virtual void visit(InsertStatement *node) = 0;
    virtual void visit(SelectStatement *node) = 0;
    virtual void visit(DeleteStatement *node) = 0;
    virtual void visit(UpdateStatement *node) = 0;
};

// AST打印器
class ASTPrinter : public ASTVisitor
{
public:
    void visit(LiteralExpression *node) override;
    void visit(IdentifierExpression *node) override;
    void visit(BinaryExpression *node) override;
    void visit(UnaryExpression *node) override;
    void visit(AggregateExpression *node) override;
    void visit(JoinClause *node) override;
    void visit(ColumnDefinition *node) override;
    void visit(CreateTableStatement *node) override;
    void visit(CreateIndexStatement *node) override;
    void visit(InsertStatement *node) override;
    void visit(SelectStatement *node) override;
    void visit(DeleteStatement *node) override;
    void visit(UpdateStatement *node) override;
};
