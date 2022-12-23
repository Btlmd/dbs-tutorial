//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_BUFFERSYSTEM_H
#define DBS_TUTORIAL_BUFFERSYSTEM_H

#include <unordered_map>
#include <map>
#include <list>
#include <utility>
#include <set>
#include <functional>

#include <defines.h>
#include <io/FileSystem.h>

struct BufferHash {
    std::size_t operator()(const std::pair<FileID, PageID> &id_pair) const {
        return (id_pair.first << 16) ^ static_cast<uint32_t>(id_pair.second);
    }
};

class BufferSystem;

class Page {
public:
    Page() = default;

    void Init(PageID _seq_id, BufferSystem *_buffer) {
        buffer = _buffer;
        seq_id = _seq_id;
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
    void Lock();

    /**
     * Allow a page to be swapped
     */
    void Release();

    /**
     * return a formatted representation of the page
     * for debug
     * @return
     */
    [[nodiscard]] std::string Seq() const {
        return " < @" + std::to_string(fd) + ", #" + std::to_string(id) + ", %" + std::to_string(seq_id) + " > ";
    }

    ~Page() = default;

    uint8_t data[PAGE_SIZE];
    bool dirty{false};
    bool lock{false};
    FileID fd{-1};
    PageID id{-1};
    PageID seq_id{-1};
    BufferSystem *buffer{nullptr};
};

class FilePageMap : public std::unordered_map<FileID, std::unordered_set<Page *>> {
public:
    void mark(FileID fd, Page *pos) {
        auto set_it{find(fd)};
        if (set_it == end()) {
            insert({fd, {pos}});
        } else {
            set_it->second.insert(pos);
        }
    }

    void drop(FileID fd, Page *pos) {
        auto set_it{find(fd)};
        assert(set_it != end());
        set_it->second.erase(pos);
    }

    void drop(FileID fd) {
        auto set_it{find(fd)};
        if (set_it != end()) {
            erase(fd);
        }
    }

    void apply(FileID fd, const std::function<void(Page *)> &func) const {
        auto set_it{find(fd)};
        if (set_it != end()) {
            for (auto it{set_it->second.begin()}; it != set_it->second.end(); ++it) {
                func(*it);
            }
        }
    }
};

class BufferSystem {
public:
    BufferSystem();

    ~BufferSystem();

    /**
     * Read an existing page
     * @param fd
     * @param page_id
     * @return Page in buffer system
     */
    Page *ReadPage(FileID fd, PageID page_id);

    /**
     * Create a new page in the buffer system
     * @param fd
     * @param page_id
     * @return Page in buffer system
     */
    Page *CreatePage(FileID fd, PageID page_id);

    /**
     * Write page back if the page is dirty
     * @param page
     */
    void WriteBack(Page *page);

    /**
     * Update file usage informantion
     * @param page
     */
    void Access(Page *page);

    /**
     * Remove entries of fd in buffer, write back if dirty
     * @param fd
     */
    void ReleaseFile(FileID fd);

    /**
     * Call BufferSystem::ReleaseFile and FileSystem::CloseFile
     * @param fd
     */
    void CloseFile(FileID fd);

private:
    /**
     * Allocate a position in the buffer system
     * @param fd
     * @param page_id
     * @return
     */
    Page *AllocPage(FileID fd, PageID page_id);

    // starting point of the continuous buffer space
    Page *buffer_;

    // from file descriptor to all relevant buffer pages
    FilePageMap buffer_map_fd_;

    // from (fd, page_id) pair to the buffer page
    std::unordered_map<std::pair<FileID, PageID>, Page *, BufferHash> buffer_map_;

    // from buffer page to its position in usage record list
    std::unordered_map<Page *, std::list<Page *>::iterator> visit_record_map_;

    // list free pages
    std::list<Page *> free_record_;

    // LRU records
    std::list<Page *> visit_record_;

    friend Page;
};

inline void Page::Lock() {
    // remove this page from visit_record_
    auto visit_record_map_it{buffer->visit_record_map_.find(this)};
    assert(!lock);
    lock = true;
    assert(visit_record_map_it != buffer->visit_record_map_.end());
    buffer->visit_record_.erase(visit_record_map_it->second);
    buffer->visit_record_map_.erase(visit_record_map_it);
}

inline void Page::Release() {
    // put this page back to visit_record_
    assert(lock);
    lock = false;
    assert(buffer->visit_record_map_.find(this) == buffer->visit_record_map_.end());
    buffer->visit_record_.push_front(this);
    buffer->visit_record_map_.insert({this, buffer->visit_record_.begin()});
}

#endif //DBS_TUTORIAL_BUFFERSYSTEM_H
