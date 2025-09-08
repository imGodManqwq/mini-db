#include "../include/cli/REPL.h"
#include <iostream>
#include <string>

REPL::REPL() : running(true) {}

void REPL::run() {
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

void REPL::handleInput(const std::string& input) {
    std::cout << "[DEBUG] Received command: " << input << "!!!!!?" << std::endl;
}