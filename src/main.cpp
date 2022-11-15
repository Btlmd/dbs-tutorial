#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include <antlr4-runtime.h>
#include <cpp-terminal/input.hpp>
#include <cpp-terminal/prompt.hpp>

#include <grammar/SQLLexer.h>
#include <grammar/SQLParser.h>

using Term::Key;
using Term::prompt_multiline;
using Term::Terminal;

void process_input(std::string &in_string) {
    antlr4::ANTLRInputStream input{in_string};
    SQLLexer lexer{&input};
    antlr4::CommonTokenStream tokens(&lexer);

    tokens.fill();
    for (auto token: tokens.getTokens()) {
        std::cout << token->toString() << std::endl;
    }

    SQLParser parser{&tokens};
    antlr4::tree::ParseTree *tree{parser.program()};

    std::cout << "Syntax Tree:" << tree->toStringTree(&parser) << std::endl << std::endl;
}

int main() {
    try {
        if (!Term::stdin_connected()) {
            std::stringstream stdin_stream{Term::read_stdin()};
            std::string batch;
            while (stdin_stream >> batch) {
                process_input(batch);
            }
            return 0;
        }

        Terminal term{false, true, false, false};
        std::cout << "Interactive prompt." << std::endl;
        std::cout << "  * Use Ctrl-D to exit." << std::endl;
        std::cout << "  * Use Enter to submit." << std::endl;
        std::cout << "  * Features:" << std::endl;
        std::cout << "    - Editing (Keys: Left, Right, Home, End, Backspace)"
                  << std::endl;
        std::cout << "    - History (Keys: Up, Down)" << std::endl;
        std::cout
                << "    - Multi-line editing"
                << std::endl;

        std::vector<std::string> history;
        std::function<bool(std::string)> iscomplete = [](std::string input_seq) -> bool {
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
            process_input(answer);
        }
    } catch (const std::runtime_error &re) {
        std::cerr << "Runtime error: " << re.what() << std::endl;
        return 2;
    } catch (...) {
        std::cerr << "Unknown error." << std::endl;
        return 1;
    }
    return 0;
}