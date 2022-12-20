//
// Created by lambda on 22-11-17.
//

#include "BufferSystem.h"
#include <boost/algorithm/string/join.hpp>

#include <cassert>

Page *BufferSystem::ReadPage(FileID fd, PageID page_id) {
    TraceLog << "BufferSystem Read @" << fd << ", #" << page_id;
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
    TraceLog << " - Use page" << pos->Seq();
    return pos;
}

BufferSystem::BufferSystem() {
    buffer_ = new Page[BUFFER_SIZE];
    for (int i{0}; i < BUFFER_SIZE; ++i) {
        buffer_[i].seq_id = i;
        free_record_.push_back(&buffer_[i]);
        visit_record_.push_back(&buffer_[i]);
        visit_record_map_.insert({&buffer_[i], std::prev(visit_record_.end())});
    }
}

void BufferSystem::WriteBack(Page *page) {
    if (page->dirty) {
        TraceLog << "Write Back Dirty" << page->Seq();
        FileSystem::WritePage(page->fd, page->id, page->data);
    }
    page->dirty = false;
}

BufferSystem::~BufferSystem() {
    delete[] buffer_;
}

void BufferSystem::Access(Page *page) {
    TraceLog << "Access" << page->Seq();
    assert(visit_record_map_.find(page) != visit_record_map_.end());
    auto it{visit_record_map_[page]};
    if (it != visit_record_.begin()) {
        visit_record_.splice(visit_record_.begin(), visit_record_, it);
    }

//    std::vector<std::string> table;
//    for (const auto &i: visit_record_) {
//        table.push_back(std::to_string(i->seq_id));
//    }
//    TraceLog << "Sequence:" << boost::algorithm::join(table, ", ");
}

void BufferSystem::ReleaseFile(FileID fd) {
    TraceLog << "Release File @" << fd;
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
    TraceLog << "Create page for @" << fd << ", #" << page_id;
    return AllocPage(fd, page_id);
}

Page *BufferSystem::AllocPage(FileID fd, PageID page_id) {
    assert(buffer_map_.find({fd, page_id}) == buffer_map_.end());
    Page *pos;
    if (buffer_map_.size() == BUFFER_SIZE) {
        assert(free_record_.empty());
        // buffer full
        pos = visit_record_.back();
        WriteBack(pos);
        buffer_map_.erase({pos->fd, pos->id});
        buffer_map_fd_.drop(pos->fd, pos);
        TraceLog << "Buffer full, drop " << pos->Seq();
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

    TraceLog << "Allocated" << pos->Seq();
    return pos;
}
