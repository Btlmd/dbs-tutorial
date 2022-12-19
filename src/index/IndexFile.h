//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXFILE_H
#define DBS_TUTORIAL_INDEXFILE_H

#include <queue>
#include <utility>
#include <defines.h>
#include <io/BufferSystem.h>
#include <index/IndexMeta.h>
#include <index/IndexPage.h>

// A Typical B+Tree structure.

class IndexFile {
   public:
    TableID table_id{-1};
    std::shared_ptr<IndexMeta> meta;
    std::vector<FieldID> field_ids;

    explicit IndexFile(TableID _table_id, std::shared_ptr<IndexMeta> _meta, std::vector<FieldID> _field_ids,
                       BufferSystem& buffer, FileID fd) :
                table_id(_table_id), meta(std::move(_meta)), field_ids(std::move(_field_ids)), buffer(buffer), fd(fd) {}

    explicit IndexFile(BufferSystem& buffer, FileID fd) : buffer{buffer}, fd{fd} {
        // Read Page 0
        class Page* page = buffer.ReadPage(fd, 0);
        const uint8_t *src = page->data;
        read_var(src, table_id);
        meta = IndexMeta::FromSrc(src);

        int field_num;
        read_var(src, field_num);
        for (int i = 0; i < field_num; ++i) {
            FieldID field_id;
            read_var(src, field_id);
            field_ids.push_back(field_id);
        }
    }

    ~IndexFile() {
        // Write back Page 0
        class Page* page = buffer.CreatePage(fd, 0);
        uint8_t *dst = page->data;
        write_var(dst, table_id);
        meta->Write(dst);
        write_var(dst, (int)field_ids.size());
        for (auto field_id : field_ids) {
            write_var(dst, field_id);
        }
        page->SetDirty();
    }

    /* Interfaces for index searching */
    // Return the position of the first element that is greater than or equal to key
    std::pair<PageID, TreeOrder> SelectRecord(const std::shared_ptr<IndexField>& key);
    void InsertRecord(PageID page_id, SlotID slot_id, const std::shared_ptr<IndexField>& key);
//    void DeleteRecord(PageID page_id_old, SlotID slot_id_old, IndexField* key);
//    void DeleteRecordRange(IndexField* key1, IndexField* key2);  // Delete [key1, key2]
//    void DeleteRecordRange(IndexField* key) { return DeleteRecordRange(key, key); }
//    void UpdateRecord(PageID page_id_old, SlotID slot_id_old, IndexField* key_old,
//                      PageID page_id, SlotID slot_id, IndexField* key) {
//        DeleteRecord(page_id_old, slot_id_old, key_old);
//        InsertRecord(page_id, slot_id, key);
//    }


    /* Interfaces for index management */
    PageID PageNum() {
        return meta->page_num;
    }

    void Print() {
        // Use BFS to print the tree
        std::queue<PageID> q;

        auto root_page_id = meta->root_page;  // Root page
        if (root_page_id < 0) {
            TraceLog << "<Empty Tree>\n"; return;
        }
        q.push(root_page_id);

        while(!q.empty()) {
            auto page_id = q.front(); q.pop();
            auto page = buffer.ReadPage(fd, page_id);
            auto index_page = std::make_shared<IndexPage>(page, *meta);
            index_page->Print();
            if (!index_page->IsLeaf()) {
                for (TreeOrder i = 0; i < index_page->ChildCount(); ++i) {
                    q.push(index_page->Select(i)->page_id);
                }
            }
        }
    }

    std::shared_ptr<IndexPage> Page(PageID page_id) {
        auto page = buffer.ReadPage(fd, page_id);
        return std::make_shared<IndexPage>(page, *meta);
    }

   private:
    BufferSystem &buffer;
    FileID fd{};

    // Find k that curr_page is the k-th child of parent_page
    TreeOrder Find(PageID parent_page_id, PageID curr_page_id) {
        auto child_cnt = Page(parent_page_id)->ChildCount();
        for (TreeOrder i = 0; i < child_cnt; ++i) {
            auto entry = Page(parent_page_id)->Select(i);
            if (entry->page_id == Page(curr_page_id)->page->id) {
                return i;
            }
        }
        return -1;
    }

    // Split the page into two pages
    void Split(PageID curr_page_id, PageID new_page_id) {
        auto child_cnt = Page(curr_page_id)->ChildCount();
        TreeOrder mid = child_cnt / 2;

        Page(new_page_id)->SetChildCount(child_cnt - mid);
        for (TreeOrder i = child_cnt - 1; i >= mid; --i) {
            auto entry = Page(curr_page_id)->Select(i);
            Page(new_page_id)->Update(i - mid, entry);
            Page(curr_page_id)->Delete(i);
        }

        // Update parent of children
        if (!Page(new_page_id)->IsLeaf()) {
            for (TreeOrder i = 0; i < Page(new_page_id)->ChildCount(); ++i) {
                auto entry = Page(new_page_id)->Select(i);
                Page(entry->page_id)->SetParentPage(Page(new_page_id)->page->id);
            }
        }
    }
};

#endif  // DBS_TUTORIAL_INDEXFILE_H
