//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_PAGE_H
#define DBS_TUTORIAL_PAGE_H

#include <defines.h>

#include <cstdint>
#include <utility>
#

struct BufferHash {
    std::size_t operator()(const std::pair<FileID, PageID> &id_pair) const {
        return id_pair.first ^ id_pair.second;
    }
};

class Page {
public:
    Page() {
        // to silence the `uninitialized` issue
        memset(data, 0xff, PAGE_SIZE);
    }

    /**
     * Mark Page as dirty
     */
    void SetDirty() {
        dirty = true;
    }

    /**
     * return a formatted representation of the page
     * for debug
     * @return
     */
    std::string Seq() const {
        return " < @" + std::to_string(fd) + ", #" + std::to_string(id) + ", %"  + std::to_string(seq_id) + " > ";
    }

    ~Page() = default;

    uint8_t data[PAGE_SIZE];
    bool dirty{false};
    FileID fd;
    PageID id;
    PageID seq_id;
};


#endif //DBS_TUTORIAL_PAGE_H
