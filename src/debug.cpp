//
// Created by lambda on 22-12-2.
//

#include <utils/System.h>

#include <iostream>
#include <string>

int main() {
    init_logger();

//    FileSystem::MakeDirectory(DB_DIR);
//    std::filesystem::remove_all(DB_DIR);

    auto dbms = DBSystem{};
    DBVisitor visitor{dbms};

    std::string buffer;
    for (std::string batch; std::getline(std::cin, batch);) {
        buffer += batch;
        if (batch.find(';') == std::string::npos) {
            continue;
        }
//        std::cout << buffer << std::endl;
        process_input(buffer, visitor);
        std::cout << std::endl;
        buffer.clear();
    }
    return 0;
}