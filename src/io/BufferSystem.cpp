//
// Created by lambda on 22-11-17.
//

#include "BufferSystem.h"
#include <boost/algorithm/string/join.hpp>

#include <cassert>

Page *BufferSystem::ReadPage(FileID fd, PageID page_id) {
    Trace("BufferSystem Read @" << fd << ", #" << page_id);
    Page *pos;
    auto lookup {buffer_map_.find({fd, page_id})};
    if (lookup == buffer_map_.end()) {
        // buffer miss
        pos = AllocPage(fd, page_id);
        FileSystem::ReadPage(fd, page_id, pos->data);
    } else {
        // buffer hit
        pos = lookup->second;
    }
    Access(pos);
    Trace(" - Use page" << pos->Seq());
    return pos;
}

BufferSystem::BufferSystem() {
    buffer_ = new Page[BUFFER_SIZE];
    for (int i{0}; i < BUFFER_SIZE; ++i) {
        buffer_[i].Init(i, this);
        free_record_.push_back(&buffer_[i]);
        visit_record_.push_back(&buffer_[i]);
        visit_record_map_.insert({&buffer_[i], std::prev(visit_record_.end())});
    }
}

void BufferSystem::WriteBack(Page *page) {
    if (page->dirty) {
        Trace("Write Back Dirty" << page->Seq());
        FileSystem::WritePage(page->fd, page->id, page->data);
    }
    page->dirty = false;
}

BufferSystem::~BufferSystem() {
    delete[] buffer_;
}

void BufferSystem::Access(Page *page) {
    Trace("Access" << page->Seq());
    auto vrm_it{visit_record_map_.find(page)};
    if(vrm_it == visit_record_map_.end()) {
        return;  // so this page is locked
    }
    auto it{vrm_it->second};
    if (it != visit_record_.begin()) {
        visit_record_.splice(visit_record_.begin(), visit_record_, it);
    }

//    std::vector<std::string> table;
//    for (const auto &i: visit_record_) {
//        table.push_back(std::to_string(i->seq_id));
//    }
//    Trace("Sequence:" << boost::algorithm::join(table, ", "));
}

void BufferSystem::ReleaseFile(FileID fd) {
    Trace("Release File @" << fd);
    // Write back page, mark the buffer space as free
    auto eraser = [this, fd](Page *pos) -> void {
        WriteBack(pos);
        assert(buffer_map_.find({fd, pos->id}) != buffer_map_.end());
        buffer_map_.erase({fd, pos->id});
        free_record_.push_front(pos);
    };
    buffer_map_fd_.apply(fd, eraser);
    buffer_map_fd_.drop(fd);
}

void BufferSystem::CloseFile(FileID fd) {
    ReleaseFile(fd);
    FileSystem::CloseFile(fd);
}

Page *BufferSystem::CreatePage(FileID fd, PageID page_id) {
    Trace("Create page for @" << fd << ", #" << page_id);
    return AllocPage(fd, page_id);
}

Page *BufferSystem::AllocPage(FileID fd, PageID page_id) {
    assert(buffer_map_.find({fd, page_id}) == buffer_map_.end());
    Page *pos;
    if (free_record_.empty()) {
        // buffer full
        pos = visit_record_.back();
        WriteBack(pos);
        buffer_map_.erase({pos->fd, pos->id});
        buffer_map_fd_.drop(pos->fd, pos);
        Trace("Buffer full, drop " << pos->Seq());
    } else {
        // with free position
        pos = free_record_.front();
        free_record_.pop_front();
    }
    buffer_map_.insert({{fd, page_id}, pos});
    buffer_map_fd_.mark(fd, pos);

    // update page info
    pos->fd = fd;
    pos->id = page_id;

    Trace("Allocated" << pos->Seq());
    return pos;
}
