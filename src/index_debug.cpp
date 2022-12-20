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
    init_logger(boost::log::trivial::severity_level::debug);
    FileSystem::RemoveDirectory(DB_DIR);
    FileSystem::MakeDirectory(DB_DIR);
    FileID index_file_fd = FileSystem::NewFile(DB_DIR / "test_index_file.bin");
    BufferSystem buffer;
    IndexFile index_file(1, std::make_shared<IndexMeta>(
                           7, 1, -1, IndexFieldType::INT2), {1, 2}, buffer, index_file_fd);

    auto GetInt2 = [](int i, int j) {
        return std::make_shared<IndexINT2>(i, false, j, false);
    };

    auto GetInt2a = [](int i) {
        return std::make_shared<IndexINT2>(i, false, i+1, false);
    };
    auto GetInt2b = [](int i) {
        return std::make_shared<IndexINT2>(i, false, i-1, false);
    };

    for (int i = 0; i < 20; ++i) {
        index_file.InsertRecord(i, i+1, GetInt2a(i));
    }

    for (int i = 0; i <= 30; ++i)
    {
        index_file.InsertRecord(i, i-1, GetInt2b(i));
    }
//    index_file.Print();
//    for (int i = 0; i <= 100; ++i) {
//        index_file.InsertRecord(1, 1, GetInt2(1, 1));  // TODO: check two-order index
//    }
    index_file.InsertRecord(1, 1, GetInt2(1, 1));  // TODO: check two-order index
    index_file.InsertRecord(1, 1, GetInt2(1, 1));
    index_file.Print();
//    buffer.CloseFile(index_file_fd);

//    index_file.Page(1)->Print();
//    index_file.Page(2)->Print();
//    index_file.Page(3)->Print();







    return 0;
}