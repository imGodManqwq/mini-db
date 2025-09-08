// 命令行接口程序
#include <iostream>
#include <string>

class REPL {
public:
    REPL() : running(true) {}

    void run() {
        std::cout << "Welcome to MiniDB! Type 'exit' to quit." << std::endl;
        std::string line;
        while (running) {
            std::cout << "db> ";
            if (!std::getline(std::cin, line)) break;
            if (line == "exit") {
                running = false;
                continue;
            }
            handleInput(line);
        }
    }

private:
    bool running;

    void handleInput(const std::string& input) {
        //  这里将来可以调用 SQLCompiler/Database/Storage
        std::cout << "[DEBUG] Received command: " << input << std::endl;
    }
};
