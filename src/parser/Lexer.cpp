#include "../../include/parser/Lexer.h"
#include <iostream>
#include <cctype>

Lexer::Lexer(const std::string &input)
    : input_(input), position_(0), line_(1), column_(1) {}

Token Lexer::nextToken()
{
    skipWhitespace();

    if (isAtEnd())
    {
        return Token(TokenType::END_OF_FILE, "", line_, column_);
    }

    char c = currentChar();

    // 处理注释
    if (c == '-' && peekChar() == '-')
    {
        skipComment();
        return nextToken(); // 递归获取下一个有效token
    }

    // 处理标识符和关键字
    if (isAlpha(c) || c == '_')
    {
        return readIdentifier();
    }

    // 处理数字
    if (isDigit(c))
    {
        return readNumber();
    }

    // 处理字符串字面量
    if (c == '\'' || c == '"')
    {
        return readStringLiteral();
    }

    // 处理运算符和分隔符
    switch (c)
    {
    case '(':
        advance();
        return Token(TokenType::LEFT_PAREN, "(", line_, column_ - 1);
    case ')':
        advance();
        return Token(TokenType::RIGHT_PAREN, ")", line_, column_ - 1);
    case ',':
        advance();
        return Token(TokenType::COMMA, ",", line_, column_ - 1);
    case ';':
        advance();
        return Token(TokenType::SEMICOLON, ";", line_, column_ - 1);
    case '.':
        advance();
        return Token(TokenType::DOT, ".", line_, column_ - 1);
    case '+':
        advance();
        return Token(TokenType::PLUS, "+", line_, column_ - 1);
    case '-':
        advance();
        return Token(TokenType::MINUS, "-", line_, column_ - 1);
    case '*':
        advance();
        return Token(TokenType::MULTIPLY, "*", line_, column_ - 1);
    case '/':
        advance();
        return Token(TokenType::DIVIDE, "/", line_, column_ - 1);
    default:
        return readOperator();
    }
}

Token Lexer::peekToken()
{
    size_t savedPos = position_;
    size_t savedLine = line_;
    size_t savedColumn = column_;

    Token token = nextToken();

    position_ = savedPos;
    line_ = savedLine;
    column_ = savedColumn;

    return token;
}

std::vector<Token> Lexer::tokenize()
{
    std::vector<Token> tokens;

    while (true)
    {
        Token token = nextToken();
        tokens.push_back(token);

        if (token.type == TokenType::END_OF_FILE || token.type == TokenType::ERROR)
        {
            break;
        }
    }

    return tokens;
}

bool Lexer::hasErrors() const
{
    return !errors_.empty();
}

const std::vector<LexerError> &Lexer::getErrors() const
{
    return errors_;
}

void Lexer::printErrors() const
{
    for (const auto &error : errors_)
    {
        std::cerr << error.toString() << std::endl;
    }
}

void Lexer::reset()
{
    position_ = 0;
    line_ = 1;
    column_ = 1;
    errors_.clear();
}

char Lexer::currentChar() const
{
    if (isAtEnd())
    {
        return '\0';
    }
    return input_[position_];
}

char Lexer::peekChar(size_t offset) const
{
    size_t peekPos = position_ + offset;
    if (peekPos >= input_.length())
    {
        return '\0';
    }
    return input_[peekPos];
}

void Lexer::advance()
{
    if (isAtEnd())
    {
        return;
    }

    if (input_[position_] == '\n')
    {
        line_++;
        column_ = 1;
    }
    else
    {
        column_++;
    }

    position_++;
}

bool Lexer::isAtEnd() const
{
    return position_ >= input_.length();
}

void Lexer::skipWhitespace()
{
    while (!isAtEnd() && isWhitespace(currentChar()))
    {
        advance();
    }
}

void Lexer::skipComment()
{
    // 跳过 '--' 注释直到行末
    while (!isAtEnd() && currentChar() != '\n')
    {
        advance();
    }
    if (!isAtEnd())
    {
        advance(); // 跳过换行符
    }
}

Token Lexer::readIdentifier()
{
    size_t startLine = line_;
    size_t startColumn = column_;
    std::string value;

    while (!isAtEnd() && (isAlphaNumeric(currentChar()) || currentChar() == '_'))
    {
        value += currentChar();
        advance();
    }

    // 检查是否为关键字
    TokenType type = KeywordMap::getKeywordType(value);

    return Token(type, value, startLine, startColumn);
}

Token Lexer::readNumber()
{
    size_t startLine = line_;
    size_t startColumn = column_;
    std::string value;
    bool hasDecimalPoint = false;

    while (!isAtEnd() && (isDigit(currentChar()) || currentChar() == '.'))
    {
        if (currentChar() == '.')
        {
            if (hasDecimalPoint)
            {
                // 第二个小数点，停止读取
                break;
            }
            hasDecimalPoint = true;
        }

        value += currentChar();
        advance();
    }

    // 检查数字格式是否正确
    if (value.empty() || value.back() == '.')
    {
        addError("Invalid number format: " + value);
        return createErrorToken(value);
    }

    TokenType type = hasDecimalPoint ? TokenType::FLOAT : TokenType::INTEGER;
    return Token(type, value, startLine, startColumn);
}

Token Lexer::readStringLiteral()
{
    size_t startLine = line_;
    size_t startColumn = column_;
    char quote = currentChar();
    std::string value;

    advance(); // 跳过开始引号

    while (!isAtEnd() && currentChar() != quote)
    {
        if (currentChar() == '\\')
        {
            // 处理转义字符
            advance();
            if (!isAtEnd())
            {
                char escaped = currentChar();
                switch (escaped)
                {
                case 'n':
                    value += '\n';
                    break;
                case 't':
                    value += '\t';
                    break;
                case 'r':
                    value += '\r';
                    break;
                case '\\':
                    value += '\\';
                    break;
                case '\'':
                    value += '\'';
                    break;
                case '"':
                    value += '"';
                    break;
                default:
                    value += escaped;
                    break;
                }
                advance();
            }
        }
        else
        {
            value += currentChar();
            advance();
        }
    }

    if (isAtEnd())
    {
        addError("Unterminated string literal");
        return createErrorToken(value);
    }

    advance(); // 跳过结束引号

    return Token(TokenType::STRING_LITERAL, value, startLine, startColumn);
}

Token Lexer::readOperator()
{
    size_t startLine = line_;
    size_t startColumn = column_;
    char c = currentChar();

    switch (c)
    {
    case '=':
        advance();
        return Token(TokenType::EQUAL, "=", startLine, startColumn);

    case '!':
        if (peekChar() == '=')
        {
            advance();
            advance();
            return Token(TokenType::NOT_EQUAL, "!=", startLine, startColumn);
        }
        break;

    case '<':
        advance();
        if (!isAtEnd() && currentChar() == '=')
        {
            advance();
            return Token(TokenType::LESS_EQUAL, "<=", startLine, startColumn);
        }
        else if (!isAtEnd() && currentChar() == '>')
        {
            advance();
            return Token(TokenType::NOT_EQUAL, "<>", startLine, startColumn);
        }
        return Token(TokenType::LESS_THAN, "<", startLine, startColumn);

    case '>':
        advance();
        if (!isAtEnd() && currentChar() == '=')
        {
            advance();
            return Token(TokenType::GREATER_EQUAL, ">=", startLine, startColumn);
        }
        return Token(TokenType::GREATER_THAN, ">", startLine, startColumn);
    }

    // 未知字符
    std::string unknownChar(1, c);
    advance();
    addError("Unknown character: " + unknownChar);
    return createErrorToken(unknownChar);
}

bool Lexer::isAlpha(char c) const
{
    return std::isalpha(c) != 0;
}

bool Lexer::isDigit(char c) const
{
    return std::isdigit(c) != 0;
}

bool Lexer::isAlphaNumeric(char c) const
{
    return isAlpha(c) || isDigit(c);
}

bool Lexer::isWhitespace(char c) const
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

void Lexer::addError(const std::string &message)
{
    errors_.emplace_back(message, line_, column_);
}

Token Lexer::createErrorToken(const std::string &value)
{
    return Token(TokenType::ERROR, value, line_, column_);
}
