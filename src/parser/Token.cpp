#include "../../include/parser/Token.h"
#include <algorithm>
#include <cctype>

// 静态成员初始化
std::unordered_map<std::string, TokenType> KeywordMap::keywords_;
bool KeywordMap::initialized_ = false;

bool Token::isKeyword() const
{
    return type >= TokenType::SELECT && type <= TokenType::DOUBLE;
}

bool Token::isOperator() const
{
    return type >= TokenType::EQUAL && type <= TokenType::DIVIDE;
}

bool Token::isLiteral() const
{
    return type == TokenType::INTEGER ||
           type == TokenType::FLOAT ||
           type == TokenType::STRING_LITERAL;
}

std::string Token::toString() const
{
    return "Token{" + tokenTypeToString(type) + ", \"" + value + "\", " +
           std::to_string(line) + ":" + std::to_string(column) + "}";
}

void KeywordMap::initializeKeywords()
{
    if (initialized_)
        return;

    keywords_ = {
        {"SELECT", TokenType::SELECT},
        {"FROM", TokenType::FROM},
        {"WHERE", TokenType::WHERE},
        {"CREATE", TokenType::CREATE},
        {"TABLE", TokenType::TABLE},
        {"INSERT", TokenType::INSERT},
        {"INTO", TokenType::INTO},
        {"VALUES", TokenType::VALUES},
        {"DELETE", TokenType::DELETE},
        {"UPDATE", TokenType::UPDATE},
        {"SET", TokenType::SET},
        {"AND", TokenType::AND},
        {"OR", TokenType::OR},
        {"NOT", TokenType::NOT},
        {"NULL", TokenType::NULL_TOKEN},
        {"TRUE", TokenType::TRUE_TOKEN},
        {"FALSE", TokenType::FALSE_TOKEN},
        {"INT", TokenType::INT},
        {"STRING", TokenType::STRING},
        {"DOUBLE", TokenType::DOUBLE},

        // GROUP BY和ORDER BY关键字
        {"GROUP", TokenType::GROUP},
        {"ORDER", TokenType::ORDER},
        {"BY", TokenType::BY},
        {"ASC", TokenType::ASC},
        {"DESC", TokenType::DESC},

        // 聚合函数关键字
        {"COUNT", TokenType::COUNT},
        {"SUM", TokenType::SUM},
        {"AVG", TokenType::AVG},
        {"MAX", TokenType::MAX},
        {"MIN", TokenType::MIN},

        // JOIN关键字
        {"JOIN", TokenType::JOIN},
        {"INNER", TokenType::INNER},
        {"LEFT", TokenType::LEFT},
        {"RIGHT", TokenType::RIGHT},
        {"FULL", TokenType::FULL},
        {"OUTER", TokenType::OUTER},
        {"ON", TokenType::ON},

        // 索引相关关键字
        {"INDEX", TokenType::INDEX},
        {"UNIQUE", TokenType::UNIQUE},

        // 约束关键字
        {"PRIMARY", TokenType::PRIMARY},
        {"KEY", TokenType::KEY},
        {"NOT", TokenType::NOT}};

    initialized_ = true;
}

TokenType KeywordMap::getKeywordType(const std::string &word)
{
    initializeKeywords();

    // 转换为大写进行匹配
    std::string upperWord = word;
    std::transform(upperWord.begin(), upperWord.end(), upperWord.begin(), ::toupper);

    auto it = keywords_.find(upperWord);
    if (it != keywords_.end())
    {
        return it->second;
    }

    return TokenType::IDENTIFIER;
}

bool KeywordMap::isKeyword(const std::string &word)
{
    return getKeywordType(word) != TokenType::IDENTIFIER;
}

std::string tokenTypeToString(TokenType type)
{
    switch (type)
    {
    case TokenType::SELECT:
        return "SELECT";
    case TokenType::FROM:
        return "FROM";
    case TokenType::WHERE:
        return "WHERE";
    case TokenType::CREATE:
        return "CREATE";
    case TokenType::TABLE:
        return "TABLE";
    case TokenType::INSERT:
        return "INSERT";
    case TokenType::INTO:
        return "INTO";
    case TokenType::VALUES:
        return "VALUES";
    case TokenType::DELETE:
        return "DELETE";
    case TokenType::UPDATE:
        return "UPDATE";
    case TokenType::SET:
        return "SET";
    case TokenType::AND:
        return "AND";
    case TokenType::OR:
        return "OR";
    case TokenType::NOT:
        return "NOT";
    case TokenType::NULL_TOKEN:
        return "NULL";
    case TokenType::TRUE_TOKEN:
        return "TRUE";
    case TokenType::FALSE_TOKEN:
        return "FALSE";
    case TokenType::INT:
        return "INT";
    case TokenType::STRING:
        return "STRING";
    case TokenType::DOUBLE:
        return "DOUBLE";

    // GROUP BY和ORDER BY关键字
    case TokenType::GROUP:
        return "GROUP";
    case TokenType::ORDER:
        return "ORDER";
    case TokenType::BY:
        return "BY";
    case TokenType::ASC:
        return "ASC";
    case TokenType::DESC:
        return "DESC";

    // 聚合函数关键字
    case TokenType::COUNT:
        return "COUNT";
    case TokenType::SUM:
        return "SUM";
    case TokenType::AVG:
        return "AVG";
    case TokenType::MAX:
        return "MAX";
    case TokenType::MIN:
        return "MIN";

    case TokenType::JOIN:
        return "JOIN";
    case TokenType::INNER:
        return "INNER";
    case TokenType::LEFT:
        return "LEFT";
    case TokenType::RIGHT:
        return "RIGHT";
    case TokenType::FULL:
        return "FULL";
    case TokenType::OUTER:
        return "OUTER";
    case TokenType::ON:
        return "ON";

    // 索引相关关键字
    case TokenType::INDEX:
        return "INDEX";
    case TokenType::UNIQUE:
        return "UNIQUE";

    // 约束关键字
    case TokenType::PRIMARY:
        return "PRIMARY";
    case TokenType::KEY:
        return "KEY";
    case TokenType::NOT_NULL:
        return "NOT_NULL";

    case TokenType::IDENTIFIER:
        return "IDENTIFIER";
    case TokenType::INTEGER:
        return "INTEGER";
    case TokenType::FLOAT:
        return "FLOAT";
    case TokenType::STRING_LITERAL:
        return "STRING_LITERAL";
    case TokenType::EQUAL:
        return "EQUAL";
    case TokenType::NOT_EQUAL:
        return "NOT_EQUAL";
    case TokenType::LESS_THAN:
        return "LESS_THAN";
    case TokenType::LESS_EQUAL:
        return "LESS_EQUAL";
    case TokenType::GREATER_THAN:
        return "GREATER_THAN";
    case TokenType::GREATER_EQUAL:
        return "GREATER_EQUAL";
    case TokenType::PLUS:
        return "PLUS";
    case TokenType::MINUS:
        return "MINUS";
    case TokenType::MULTIPLY:
        return "MULTIPLY";
    case TokenType::DIVIDE:
        return "DIVIDE";
    case TokenType::LEFT_PAREN:
        return "LEFT_PAREN";
    case TokenType::RIGHT_PAREN:
        return "RIGHT_PAREN";
    case TokenType::COMMA:
        return "COMMA";
    case TokenType::SEMICOLON:
        return "SEMICOLON";
    case TokenType::DOT:
        return "DOT";
    case TokenType::END_OF_FILE:
        return "END_OF_FILE";
    case TokenType::UNKNOWN:
        return "UNKNOWN";
    case TokenType::ERROR:
        return "ERROR";
    default:
        return "UNKNOWN";
    }
}
