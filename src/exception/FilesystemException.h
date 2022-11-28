//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_SYSTEM_EXCEPTION_H
#define DBS_TUTORIAL_SYSTEM_EXCEPTION_H

#include <stdexcept>
#include <fmt/core.h>

class FileSystemError : public std::runtime_error {
public:
    template<typename... T>
    explicit FileSystemError(fmt::format_string<T...> fmt, T &&... args) :
            std::runtime_error{"FileSystemError: " + fmt::format(fmt, args...)} {
        ErrorLog << std::runtime_error::what();
    }
};


#endif //DBS_TUTORIAL_SYSTEM_EXCEPTION_H
