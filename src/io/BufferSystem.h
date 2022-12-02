//
// Created by lambda on 22-11-17.
//

#ifndef DBS_TUTORIAL_BUFFERSYSTEM_H
#define DBS_TUTORIAL_BUFFERSYSTEM_H

#include <unordered_map>
#include <map>
#include <list>
#include <utility>

#include <defines.h>
#include <io/Page.h>
#include <io/FileSystem.h>


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

    Page *buffer_;
    std::multimap<FileID, Page *> buffer_map_fd_;
//    std::unordered_map<std::pair<FileID, PageID>, Page *, BufferHash> buffer_map_;
    std::map<std::pair<FileID, PageID>, Page *> buffer_map_;
    std::map<Page *, std::list<Page *>::iterator> visit_record_map_;
    std::list<Page *> free_record_;
    std::list<Page *> visit_record_;
    FileSystem page_fs_;
};


#endif //DBS_TUTORIAL_BUFFERSYSTEM_H
