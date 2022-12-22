//
// Created by lambda on 22-11-19.
//

#ifndef DBS_TUTORIAL_OPERATIONEXCEPTION_H
#define DBS_TUTORIAL_OPERATIONEXCEPTION_H

#include <defines.h>

#include <stdexcept>
#include <fmt/core.h>

class OperationError : public std::runtime_error {
public:
    template<typename... T>
    explicit OperationError(fmt::format_string<T...> fmt, T &&... args) :
            std::runtime_error{fmt::format(fmt, std::forward<decltype(args)>(args) ...)} {
        ErrorLog << std::runtime_error::what();
    }
};

#endif //DBS_TUTORIAL_OPERATIONEXCEPTION_H
