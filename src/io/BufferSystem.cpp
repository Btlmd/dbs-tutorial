//
// Created by lambda on 22-11-17.
//

#include "BufferSystem.h"

#include <cassert>

Page *BufferSystem::ReadPage(FileID fd, PageID page_id) {
    TraceLog << "Read page @" << fd << " #" << page_id;
    Page *pos;
    auto lookup {buffer_map_.find({fd, page_id})};
    if (lookup == buffer_map_.end()) {
        // buffer miss
        AllocPage(fd, page_id);
        FileSystem::ReadPage(fd, page_id, pos->data);
    } else {
        // buffer hit
        pos = lookup->second;
    }
    Access(pos);
    return pos;
}

BufferSystem::BufferSystem() {
    buffer_ = new Page[BUFFER_SIZE];
    for (int i{0}; i < BUFFER_SIZE; ++i) {
        free_record_.push_back(&buffer_[i]);
    }
}

void BufferSystem::WriteBack(Page *page) {
    if (page->dirty) {
        FileSystem::WritePage(page->fd, page->id, page->data);
    }
    page->dirty = false;
}

BufferSystem::~BufferSystem() {
    delete[] buffer_;
}

void BufferSystem::Access(Page *page) {
    assert(visit_record_map_.find(page) != visit_record_map_.end());
    auto it{visit_record_map_[page]};
    auto page_ptr{*it};
    visit_record_.erase(it);
    visit_record_.push_front(page_ptr);
}

void BufferSystem::ReleaseFile(FileID fd) {
    for (auto [ptr, end]{buffer_map_fd_.equal_range(fd)}; ptr != end; ++ptr) {
        WriteBack(ptr->second);
        assert(buffer_map_.find({fd, ptr->second->id}) != buffer_map_.end());
        buffer_map_.erase({fd, ptr->second->id});
    }
    buffer_map_fd_.erase(fd);
}

void BufferSystem::CloseFile(FileID fd) {
    ReleaseFile(fd);
    FileSystem::CloseFile(fd);
}

Page *BufferSystem::CreatePage(FileID fd, PageID page_id) {
    TraceLog << "New page @" << fd << " #" << page_id;
    return AllocPage(fd, page_id);
}

Page *BufferSystem::AllocPage(FileID fd, PageID page_id) {
    assert(buffer_map_.find({fd, page_id}) == buffer_map_.end());
    Page *pos;
    if (visit_record_.size() == BUFFER_SIZE) {
        assert(free_record_.empty());
        // buffer full
        pos = visit_record_.back();
        WriteBack(pos);
    } else {
        // with free position
        pos = free_record_.front();
        free_record_.pop_front();
    }
    buffer_map_.insert({{fd, page_id}, pos});
    buffer_map_fd_.insert({fd, pos});
    return pos;
}
