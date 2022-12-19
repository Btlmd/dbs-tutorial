//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_FILESYSTEM_H
#define DBS_TUTORIAL_FILESYSTEM_H

#include <defines.h>
#include <string>
#include <cstdint>


class FileSystem {
public:
    // directory operation

    static void MakeDirectory(const std::string &, bool = true);

    static void RemoveDirectory(const std::string &);

    // file operation

    static FileID NewFile(const std::string &);

    static void RemoveFile(const std::string &);

    static FileID OpenFile(const std::string &);

    static void CloseFile(FileID fd);

    // page opertation

    static void WritePage(FileID fd, PageID page_id, uint8_t *src);

    static void ReadPage(FileID fd, PageID page_id, uint8_t *dst);
};


#endif //DBS_TUTORIAL_FILESYSTEM_H
