#pragma once
#include "AST.h"
#include "Catalog.h"
#include <vector>
#include <string>
#include <memory>

// 语义错误类型
enum class SemanticErrorType
{
    TABLE_NOT_EXISTS,      // 表不存在
    TABLE_ALREADY_EXISTS,  // 表已存在
    COLUMN_NOT_EXISTS,     // 列不存在
    COLUMN_ALREADY_EXISTS, // 列已存在
    TYPE_MISMATCH,         // 类型不匹配
    COLUMN_COUNT_MISMATCH, // 列数不匹配
    INVALID_DATA_TYPE,     // 无效的数据类型
    DUPLICATE_COLUMN_NAME, // 重复的列名
    EMPTY_TABLE_NAME,      // 空表名
    EMPTY_COLUMN_NAME,     // 空列名
    INVALID_VALUE,         // 无效的值
    MISSING_PRIMARY_KEY,   // 缺少主键
    DUPLICATE_PRIMARY_KEY, // 重复的主键
    AMBIGUOUS_COLUMN,      // 列名模糊（多表查询时）
    INVALID_FUNCTION,      // 无效的函数
    UNKNOWN_ERROR          // 未知错误
};

// 语义错误信息
struct SemanticError
{
    SemanticErrorType type;
    std::string message;
    int line;
    int column;
    std::string location; // 错误发生的具体位置描述

    SemanticError(SemanticErrorType t, const std::string &msg, int l = 0, int c = 0, const std::string &loc = "")
        : type(t), message(msg), line(l), column(c), location(loc) {}

    std::string toString() const;
};

// 语义分析结果
struct SemanticAnalysisResult
{
    bool success;
    std::vector<SemanticError> errors;
    std::vector<std::string> warnings;

    SemanticAnalysisResult() : success(true) {}

    void addError(const SemanticError &error)
    {
        errors.push_back(error);
        success = false;
    }

    void addWarning(const std::string &warning)
    {
        warnings.push_back(warning);
    }

    bool hasErrors() const { return !errors.empty(); }
    bool hasWarnings() const { return !warnings.empty(); }
};

// 语义分析器
class SemanticAnalyzer : public ASTVisitor
{
public:
    explicit SemanticAnalyzer(std::shared_ptr<Catalog> catalog);
    ~SemanticAnalyzer() = default;

    // 主要分析接口
    SemanticAnalysisResult analyzeStatement(Statement *stmt);
    SemanticAnalysisResult analyzeStatements(const std::vector<std::unique_ptr<Statement>> &statements);

    // 表达式类型推导
    DataType inferExpressionType(Expression *expr, const std::string &contextTable = "");

    // 值类型验证
    bool validateValue(const Value &value, DataType expectedType);

    // 访问者模式实现
    void visit(LiteralExpression *node) override;
    void visit(IdentifierExpression *node) override;
    void visit(BinaryExpression *node) override;
    void visit(UnaryExpression *node) override;
    void visit(AggregateExpression *node) override;
    void visit(JoinClause *node) override;
    void visit(ColumnDefinition *node) override;
    void visit(CreateTableStatement *node) override;
    void visit(DropTableStatement *node) override;
    void visit(InsertStatement *node) override;
    void visit(SelectStatement *node) override;
    void visit(DeleteStatement *node) override;
    void visit(UpdateStatement *node) override;
    void visit(CreateIndexStatement *node) override;

    // 错误报告
    const std::vector<SemanticError> &getErrors() const { return result_.errors; }
    const std::vector<std::string> &getWarnings() const { return result_.warnings; }
    bool hasErrors() const { return result_.hasErrors(); }
    void printErrors() const;
    void clearErrors() { result_ = SemanticAnalysisResult(); }

    // 获取Catalog
    std::shared_ptr<Catalog> getCatalog() const { return catalog_; }

private:
    std::shared_ptr<Catalog> catalog_;
    SemanticAnalysisResult result_;
    std::string currentTable_; // 当前分析的表名（用于上下文）

    // 辅助方法
    void addError(SemanticErrorType type, const std::string &message, const std::string &location = "");
    void addWarning(const std::string &warning);

    // 特定语句分析
    void analyzeCreateTable(CreateTableStatement *stmt);
    void analyzeDropTable(DropTableStatement *stmt);
    void analyzeInsert(InsertStatement *stmt);
    void analyzeSelect(SelectStatement *stmt);
    void analyzeDelete(DeleteStatement *stmt);
    void analyzeUpdate(UpdateStatement *stmt);
    void analyzeCreateIndex(CreateIndexStatement *stmt);

    // 表达式分析
    void analyzeExpression(Expression *expr, const std::string &contextTable = "");
    void analyzeBinaryExpression(BinaryExpression *expr, const std::string &contextTable = "");
    void analyzeIdentifier(IdentifierExpression *expr, const std::string &contextTable = "");
    void analyzeJoinCondition(Expression *expr, const std::string &leftTable, const std::string &rightTable);

    // 类型检查
    bool isNumericType(DataType type) const;
    bool isComparableTypes(DataType left, DataType right) const;
    bool isArithmeticCompatible(DataType left, DataType right) const;
    DataType getResultType(DataType left, DataType right, TokenType op) const;

    // 错误类型转换
    std::string semanticErrorTypeToString(SemanticErrorType type) const;
};
