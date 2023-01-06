//
// Created by lambda on 22-12-2.
//

#ifndef DBS_TUTORIAL_SYSTEM_G_H
#define DBS_TUTORIAL_SYSTEM_G_H

#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>
#include <filesystem>

#include <antlr4-runtime.h>
#include <cpp-terminal/input.hpp>
#include <cpp-terminal/prompt.hpp>
#include <fmt/chrono.h>
#include <fmt/core.h>

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
#include <system/DBVisitor.h>
#include <exception/OperationException.h>

ResultList ToResultList(const std::string &in_string, DBVisitor &visitor, bool parsing_time = false) {
    InfoLog << in_string.substr(0, 1024);
    auto begin{std::chrono::high_resolution_clock::now()};
    antlr4::ANTLRInputStream input{in_string};
    SQLLexer lexer{&input};
    antlr4::CommonTokenStream tokens{&lexer};
    tokens.fill();
    SQLParser parser{&tokens};
    antlr4::tree::ParseTree *tree{parser.program()};
    std::chrono::duration<double> elapse{std::chrono::high_resolution_clock::now() - begin};
    if (parsing_time) {
        fmt::print("parsing takes {:.2f} sec\n", elapse.count());
    }
    if (parser.getNumberOfSyntaxErrors() > 0) {
        throw OperationError{"Syntax Error"};
    }
    Trace("Syntax Tree:" << tree->toStringTree(&parser));
    auto results{tree->accept(&visitor)};
    auto &result_list{*results.as<std::shared_ptr<ResultList>>()};
    return result_list;
}


void inline process_input(const std::string &in_string, DBVisitor &visitor, bool beep = false) {
    try {
        auto result_list{ToResultList(in_string, visitor, false)};
        for (auto &result_ptr: result_list) {
            std::cout << result_ptr->Display() << std::endl;
            std::cout.flush();
        }
    } catch (const OperationError &e) {
        if (beep) {
            std::cout << '\a';
        }
        std::cout << "Error: " << e.what() << std::endl;
    }
    // catch (...)
    // left uncaught exception to upper level
}

void inline init_logger(bool clog = false) {
    namespace logging = boost::log;
    namespace expr = boost::log::expressions;
    namespace sinks = boost::log::sinks;

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
    if (clog) {
        backend->add_stream(
                boost::shared_ptr<std::ostream>(
                        &std::clog, boost::null_deleter()
                )
        );
    }

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

#endif //DBS_TUTORIAL_SYSTEM_G_H
