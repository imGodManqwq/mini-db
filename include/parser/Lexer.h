#pragma once
#include "Token.h"
#include <string>
#include <vector>
#include <memory>

// 词法分析错误类
class LexerError
{
public:
    std::string message;
    size_t line;
    size_t column;

    LexerError(const std::string &msg, size_t l, size_t c)
        : message(msg), line(l), column(c) {}

    std::string toString() const
    {
        return "Lexer Error at " + std::to_string(line) + ":" + std::to_string(column) + " - " + message;
    }
};

// 词法分析器类
class Lexer
{
public:
    explicit Lexer(const std::string &input);
    ~Lexer() = default;

    // 获取下一个token
    Token nextToken();

    // 预览下一个token（不移动位置）
    Token peekToken();

    // 获取所有tokens
    std::vector<Token> tokenize();

    // 检查是否有错误
    bool hasErrors() const;

    // 获取错误列表
    const std::vector<LexerError> &getErrors() const;

    // 打印所有错误
    void printErrors() const;

    // 重置词法分析器到开始位置
    void reset();

private:
    std::string input_;
    size_t position_; // 当前字符位置
    size_t line_;     // 当前行号
    size_t column_;   // 当前列号
    std::vector<LexerError> errors_;

    // 当前字符相关方法
    char currentChar() const;
    char peekChar(size_t offset = 1) const;
    void advance();
    bool isAtEnd() const;

    // 跳过空白字符
    void skipWhitespace();

    // 跳过注释
    void skipComment();

    // 读取标识符或关键字
    Token readIdentifier();

    // 读取数字（整数或浮点数）
    Token readNumber();

    // 读取字符串字面量
    Token readStringLiteral();

    // 读取运算符
    Token readOperator();

    // 字符判断辅助方法
    bool isAlpha(char c) const;
    bool isDigit(char c) const;
    bool isAlphaNumeric(char c) const;
    bool isWhitespace(char c) const;

    // 错误处理
    void addError(const std::string &message);
    Token createErrorToken(const std::string &message);
};
