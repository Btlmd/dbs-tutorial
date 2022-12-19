//
// Created by c7w on 2022/12/19.
//


#include <utils/System.h>

#include <iostream>
#include <string>
#include <io/BufferSystem.h>
#include <io/FileSystem.h>
#include <index/IndexFile.h>

int main() {
    init_logger();
    FileSystem::RemoveDirectory(DB_DIR);
    FileSystem::MakeDirectory(DB_DIR);
    FileID index_file_fd = FileSystem::NewFile(DB_DIR / "test_index_file.bin");
    BufferSystem buffer;
    IndexFile index_file(1, std::make_shared<IndexMeta>(
                           7, 1, -1, IndexFieldType::INT), {1}, buffer, index_file_fd);

    auto GetInt = [](int i) {
        return std::make_shared<IndexINT>(i, false);
    };

    for (int i = 0; i < 3; ++i) {
        index_file.InsertRecord(i, i+1, GetInt(i));
    }


    index_file.Print();

    return 0;
}