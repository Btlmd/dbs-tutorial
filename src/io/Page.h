//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_PAGE_H
#define DBS_TUTORIAL_PAGE_H

#include <defines.h>
#include <cstdint>
#include <utility>

struct BufferHash {
    std::size_t operator()(const std::pair<FileID, PageID> &id_pair) const {
        return id_pair.first ^ id_pair.second;
    }
};

class Page {
public:
    Page() = default;

    void SetDirty() {
        dirty = true;
    }

    ~Page() = default;

    uint8_t data[PAGE_SIZE];
    bool dirty;
    FileID fd;
    PageID id;
};


#endif //DBS_TUTORIAL_PAGE_H
