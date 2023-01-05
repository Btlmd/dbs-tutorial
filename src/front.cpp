#include <utils/System.h>

#include <iostream>
#include <string>
#include <functional>

#include <system/DBSystem.h>
#include <system/DBVisitor.h>
#include <exception/OperationException.h>

using Term::Key;
using Term::prompt_multiline;
using Term::Terminal;


int main() {
    init_logger();

    auto dbms = DBSystem{};
    DBVisitor visitor{dbms};
    try {
        if (!Term::stdin_connected()) {

        }

        Terminal term{false, true, false, false};
        std::cout << "[dbs-tutorial] project by lambda & c7w" << std::endl;
        std::cout << "Usage: Enter to submit, Ctrl-D to exit" << std::endl;
        std::cout << "       Multi-line editing and history are supported" << std::endl;

        std::vector<std::string> history;
        std::function<bool(std::string)> iscomplete = [](const std::string& input_seq) -> bool {
            return input_seq.find(';') != std::string::npos;
        };
        while (true) {
            std::string answer{
                    Term::prompt_multiline(term, "sql> ", history, iscomplete)
            };
            if (answer.size() == 1 && answer[0] == Key::CTRL_D) {
                std::cout << "Bye :)" << std::endl;
                break;
            }
            process_input(answer, visitor, true);
        }
    } catch (const std::exception &e) {
        std::cerr << "Unexpected error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}