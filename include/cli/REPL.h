#pragma once
#include <string>

class REPL {
public:
    REPL();                 // 构造函数
    void run();             // 启动 REPL 主循环

private:
    bool running;           // 标志是否继续运行
    void handleInput(const std::string& input);  // 处理输入
};