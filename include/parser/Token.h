#pragma once
#include <string>
#include <unordered_map>

// Token类型枚举
enum class TokenType {
    // 关键字
    SELECT,
    FROM,
    WHERE,
    CREATE,
    TABLE,
    INSERT,
    INTO,
    VALUES,
    DELETE,
    UPDATE,
    SET,
    AND,
    OR,
    NOT,
    NULL_TOKEN,
    TRUE_TOKEN,
    FALSE_TOKEN,
    
    // GROUP BY和ORDER BY关键字
    GROUP,
    ORDER,
    BY,
    ASC,
    DESC,
    
    // 聚合函数关键字
    COUNT,
    SUM,
    AVG,
    MAX,
    MIN,
    
    // JOIN关键字
    JOIN,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    OUTER,
    ON,
    
    // 索引相关关键字
    INDEX,
    UNIQUE,
    
    // 约束关键字
    PRIMARY,
    KEY,
    NOT_NULL,
    
    // 数据类型关键字
    INT,
    STRING,
    DOUBLE,
    
    // 标识符和字面量
    IDENTIFIER,     // 表名、列名等
    INTEGER,        // 整数常量
    FLOAT,          // 浮点数常量
    STRING_LITERAL, // 字符串字面量
    
    // 运算符
    EQUAL,          // =
    NOT_EQUAL,      // !=, <>
    LESS_THAN,      // <
    LESS_EQUAL,     // <=
    GREATER_THAN,   // >
    GREATER_EQUAL,  // >=
    PLUS,           // +
    MINUS,          // -
    MULTIPLY,       // *
    DIVIDE,         // /
    
    // 分隔符
    LEFT_PAREN,     // (
    RIGHT_PAREN,    // )
    COMMA,          // ,
    SEMICOLON,      // ;
    DOT,            // .
    
    // 特殊标记
    END_OF_FILE,    // 文件结束
    UNKNOWN,        // 未知token
    
    // 错误标记
    ERROR           // 错误token
};

// Token结构
struct Token {
    TokenType type;
    std::string value;
    size_t line;
    size_t column;
    
    Token(TokenType t = TokenType::UNKNOWN, const std::string& v = "", size_t l = 1, size_t c = 1)
        : type(t), value(v), line(l), column(c) {}
    
    // 判断是否为关键字
    bool isKeyword() const;
    
    // 判断是否为运算符
    bool isOperator() const;
    
    // 判断是否为字面量
    bool isLiteral() const;
    
    // 转换为字符串表示
    std::string toString() const;
};

// 关键字映射表
class KeywordMap {
public:
    static TokenType getKeywordType(const std::string& word);
    static bool isKeyword(const std::string& word);
    
private:
    static std::unordered_map<std::string, TokenType> keywords_;
    static void initializeKeywords();
    static bool initialized_;
};

// Token类型到字符串的转换
std::string tokenTypeToString(TokenType type);
