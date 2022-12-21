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
     * Make sure a page will not be swapped
     * Note: since we lock it in list head, you must make sure that you lock the page immediately after `Access` it
     */
    void Lock() {
        if (!lock) {
            ++lock_count;
        }
        lock = true;
    }

    /**
     * Allow a page to be swapped
     */
    void Release() {
        if (lock) {
            --lock_count;
        }
        lock = false;
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
    bool lock{false};
    FileID fd;
    PageID id;
    PageID seq_id;
    static std::size_t lock_count;
};


#endif //DBS_TUTORIAL_PAGE_H
