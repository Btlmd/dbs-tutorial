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
    init_logger(true);
    FileSystem::RemoveDirectory(DB_DIR);
    FileSystem::MakeDirectory(DB_DIR);
    FileID index_file_fd = FileSystem::NewFile(DB_DIR / "test_index_file.bin");
    BufferSystem buffer;
    IndexFile index_file(1, std::make_shared<IndexMeta>(
                           233, 1, -1, IndexFieldType::INT2), {1, 2}, buffer, index_file_fd);

    auto GetInt2 = [](int i, int j) {
        return std::make_shared<IndexINT2>(i, false, j, false);
    };

    auto GetInt2a = [](int i) {
        return std::make_shared<IndexINT2>(i, false, i+1, false);
    };
    auto GetInt2b = [](int i) {
        return std::make_shared<IndexINT2>(i, false, i-1, false);
    };


    // Time start
    auto start = std::chrono::system_clock::now();
    for (int i = 0; i < 1000; ++i) {
        for (int j = 0; j < 1000; ++j) {
            index_file.InsertRecord(i, j, GetInt2(i, j));
        }
    }
    // Time end
    auto end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    DebugLog << "Time cost: " << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den << "s" << std::endl;
    DebugLog << "Page count: " << index_file.meta->page_num;

    // Start search
    // Random a number between 0 and 1999
    start = std::chrono::system_clock::now();
    for (int k = 0; k < 100000; ++k) {
        int i = rand() % 1000, j = rand() % 1000;
        auto iter = index_file.SelectRecord(GetInt2(i, j));
        auto record = index_file.Select(iter);
        if (record->page_id != i || record->slot_id != j) {
            DebugLog << "Error: " << i << " " << j << " " << record->page_id << " " << record->slot_id;
        }
    }
    end = std::chrono::system_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    DebugLog << "Time cost: " << double(duration.count()) * std::chrono::microseconds::period::num / std::chrono::microseconds::period::den << "s" << std::endl;

    buffer.ReleaseFile(index_file_fd);

    return 0;
}