#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>

#include <antlr4-runtime.h>
#include <cpp-terminal/input.hpp>
#include <cpp-terminal/prompt.hpp>
#include <fmt/chrono.h>

#include <boost/log/core.hpp>
#include <boost/core/null_deleter.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/lambda/lambda.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/common.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sources/logger.hpp>

#include <defines.h>
#include <grammar/SQLLexer.h>
#include <grammar/SQLParser.h>
#include "system/DBVisitor.h"
#include <system/DBSystem.h>
#include <exception/OperationException.h>

namespace logging = boost::log;
namespace expr = boost::log::expressions;
namespace sinks = boost::log::sinks;
using Term::Key;
using Term::prompt_multiline;
using Term::Terminal;

void process_input(std::string &in_string, DBVisitor &visitor) {
    try {
        antlr4::ANTLRInputStream input{in_string};
        SQLLexer lexer{&input};
        antlr4::CommonTokenStream tokens(&lexer);
        tokens.fill();
        SQLParser parser{&tokens};
        antlr4::tree::ParseTree *tree{parser.program()};
        if (parser.getNumberOfSyntaxErrors() > 0) {
            throw OperationError{"Syntax Error"};
        }
        TraceLog << "Syntax Tree:" << tree->toStringTree(&parser);
        auto results{tree->accept(&visitor)};
        auto &result_list{*results.as<std::shared_ptr<ResultList>>()};
        for (auto &result_ptr: result_list) {
            std::cout << result_ptr->ToString() << std::endl;
        }
    } catch (const OperationError &e) {
        std::cout << e.what() << std::endl;
    }
}

void init_logger() {
    boost::shared_ptr<logging::core> core = logging::core::get();
    typedef sinks::synchronous_sink<sinks::text_ostream_backend> sink_t;

    boost::shared_ptr<sinks::text_ostream_backend> backend =
            boost::make_shared<sinks::text_ostream_backend>();

    backend->add_stream(
            boost::shared_ptr<std::ostream>(
                    new std::ofstream(
                            fmt::format(LOGGING_PATTERN, fmt::localtime(std::time(nullptr)))
                    )
            )
    );

    backend->add_stream(
            boost::shared_ptr<std::ostream>(
                    &std::clog, boost::null_deleter()
            )
    );

    // auto-flush
    backend->auto_flush(true);
    boost::shared_ptr<sink_t> sink(new sink_t(backend));

    // logging format
    sink->set_formatter(
            expr::format("%1% [%2%] %3%")
            % expr::attr<boost::posix_time::ptime>("TimeStamp")
            % logging::trivial::severity
            % expr::smessage
    );
    core->add_sink(sink);
    core->set_filter(logging::trivial::severity >= SEVERITY);
    logging::add_common_attributes();
}

//int main() {
//    init_logger();
//
//    // init system
//    auto dbms = DBSystem{};
//    DBVisitor visitor{dbms};
//
//    std::string batch{"CREATE DATABASE TESTDB;"};
//    process_input(batch, visitor);
//    return 0;
//
//}
int main() {
    init_logger();

    FileSystem::MakeDirectory(DB_DIR);
    std::filesystem::remove_all(DB_DIR);

    auto dbms = DBSystem{};
    DBVisitor visitor{dbms};
//    try {
#ifdef DEBUG
    {
#else
        if (!Term::stdin_connected()) {
#endif
//        std::string batch{
//            "CREATE DATABASE test_db;"
//                          "USE test_db;"
//                          "CREATE TABLE test_table0(\n"
//                          "     int_f0 INT,\n"
//                          "     int_f1 INT,\n"
//                          "     vc_f VARCHAR(20) NOT NULL,\n"
//                          "     float_f FLOAT,\n"
//                          "\n"
//                          "     PRIMARY KEY pk_name (int_f0, int_f1)\n"
////                          "    FOREIGN KEY fk_name (int_f0, int_f1) REFERENCES tt(int_f2, int_f3)\n"
//                          ");"};
//        process_input(batch, visitor);
        for (std::string batch; std::getline(std::cin, batch);) {
            process_input(batch, visitor);
        }
#ifdef DEBUG
        return 0;
#endif
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
        process_input(answer, visitor);
    }
//    } catch (const std::runtime_error &re) {
//        std::cerr << "Runtime error: " << re.what() << std::endl;
//        return 2;
//    } catch (...) {
//        std::cerr << "Unknown error." << std::endl;
//        return 1;
//    }
    return 0;
}