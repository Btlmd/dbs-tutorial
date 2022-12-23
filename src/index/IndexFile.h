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

    // Constructor for new index file
    explicit IndexFile(TableID _table_id, std::shared_ptr<IndexMeta> _meta, std::vector<FieldID> _field_ids,
                       BufferSystem &buffer, FileID fd) :
            table_id(_table_id), meta(std::move(_meta)), field_ids(std::move(_field_ids)), buffer(buffer), fd(fd) {
        class Page *page = buffer.CreatePage(fd, 0);
        Write();
    }

    // Constructor for loading existing index file (Read fd from DBSystem.table_index_fd)
    explicit IndexFile(BufferSystem &buffer, FileID fd) : buffer{buffer}, fd{fd} {
        // Read Page 0
        class Page *page = buffer.ReadPage(fd, 0);
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
        Write();
    }

    void Write() {
        class Page *page = buffer.ReadPage(fd, 0);
        uint8_t *dst = page->data;
        write_var(dst, table_id);
        meta->Write(dst);
        write_var(dst, (int) field_ids.size());
        for (auto field_id: field_ids) {
            write_var(dst, field_id);
        }
        page->SetDirty();
    }

    /* Interfaces for index searching */
    // Return the position of the first element that is greater than or equal to key
    std::pair<PageID, TreeOrder> Next(std::pair<PageID, TreeOrder> iter);

    std::shared_ptr<IndexRecordLeaf> Select(std::pair<PageID, TreeOrder> iter);

    std::pair<PageID, TreeOrder> SelectRecord(const std::shared_ptr<IndexField> &key);

    std::pair<PageID, TreeOrder>
    SelectRecordExact(const std::shared_ptr<IndexField> &key);  // Return {-1, -1} if no exact match

    [[nodiscard]] bool Contains(const std::shared_ptr<IndexField> &key) {
        return SelectRecordExact(key) != std::make_pair<PageID, TreeOrder>(-1, -1);
    }

    void InsertRecord(PageID page_id, SlotID slot_id, const std::shared_ptr<IndexField> &key);

    int DeleteRecord(PageID page_id, SlotID slot_id, const std::shared_ptr<IndexField> &key);

    int DeleteRecordRange(const std::shared_ptr<IndexField> &key1,
                          const std::shared_ptr<IndexField> &key2);  // Delete [key1, key2]
    int DeleteRecordRange(const std::shared_ptr<IndexField> &key) { return DeleteRecordRange(key, key); }

    void UpdateRecord(PageID page_id, SlotID slot_id, const std::shared_ptr<IndexField> &key) {
        DeleteRecord(page_id, slot_id, key);
        InsertRecord(page_id, slot_id, key);
    }

    // Returns the next valid page after deletion for searching (here `next` means it is possible to meet new elements)
    // In case of not underflow, return the next page
    // In case of borrowing, return the current page
    // In case of merging, return the next page
    PageID SolveDelete(PageID delete_page_id);


    /* Interfaces for index management */
    PageID PageNum() {
        return meta->page_num;
    }

    void Print() {
        // Use BFS to print the tree
        std::queue<PageID> q;

        auto root_page_id = meta->root_page;  // Root page
        if (root_page_id < 0) {
            DebugLog << "<Empty Tree>\n";
            return;
        }
        q.push(root_page_id);

        while (!q.empty()) {
            auto page_id = q.front();
            q.pop();
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
        // First decide if page_cache is too large
        if (page_cache.size() > 10) {
            Release();
        }

        // First find in page_cache
        if (page_cache.find(page_id) != page_cache.end()) {
            return page_cache[page_id];
        } else {
            auto page = buffer.ReadPage(fd, page_id);
            page->Lock();
            auto index_page = std::make_shared<IndexPage>(page, *meta);
            page_cache[page_id] = index_page;
            return index_page;
        }
    }

    void Release() {
        for (auto &item: page_cache) {
            item.second->page->Release();
        }
        page_cache.clear();
    }

private:
    std::unordered_map<PageID, std::shared_ptr<IndexPage>> page_cache;
    BufferSystem &buffer;
    FileID fd{};

    // Find k that curr_page is the k-th child of parent_page
    TreeOrder Find(PageID parent_page_id, PageID curr_page_id) {

        // First get key from curr_page_id
        auto key = Page(curr_page_id)->Select(Page(curr_page_id)->ChildCount()-1)->key;

        // Then find the key in parent_page_id
        auto find_pos = Find(parent_page_id, key);

        auto child_cnt = Page(parent_page_id)->ChildCount();
        for (TreeOrder i = find_pos; i < child_cnt; ++i) {
            auto entry = Page(parent_page_id)->Select(i);
            if (entry->page_id == Page(curr_page_id)->page->id) {
                return i;
            }
            if (*entry->key > *key) {
                break;
            }
        }
        return -1;
    }

    // Use binary search to find the first children of Page(page_id) that is greater than or equal to key
    // Return the last child if key is greater than all children
    TreeOrder Find(PageID page_id, const std::shared_ptr<IndexField>& key) {
        auto child_cnt = Page(page_id)->ChildCount();
        TreeOrder ret_candidate = child_cnt - 1;

        TreeOrder l = 0, r = child_cnt - 1;  // [l, r]
        while (l <= r) {  // Use binary search to find the first children of Page(page_id) that is greater than or equal to key
            TreeOrder mid = (l + r) / 2;
            auto mid_entry = Page(page_id)->Select(mid);
            auto entry_key = Page(page_id)->CastRecord(mid_entry)->key;

            if (*entry_key < *key) {
                l = mid + 1;
            } else {
                r = mid - 1;
                ret_candidate = mid;
            }
        }
        assert (0 <= ret_candidate && ret_candidate < child_cnt);
        return ret_candidate;
    }

    // Split the page into two pages
    void Split(PageID curr_page_id, PageID new_page_id) {
        auto child_cnt = Page(curr_page_id)->ChildCount();
        TreeOrder mid = child_cnt / 2;

        Page(new_page_id)->InsertRange(0, Page(curr_page_id)->SelectRange(mid, child_cnt - 1));
        Page(curr_page_id)->DeleteRange(mid, child_cnt - 1);

        // Update parent of children
        if (!Page(new_page_id)->IsLeaf()) {
            for (TreeOrder i = 0; i < Page(new_page_id)->ChildCount(); ++i) {
                auto entry = Page(new_page_id)->Select(i);
                Page(entry->page_id)->SetParentPage(Page(new_page_id)->page->id);
            }
        }
    }

    bool Borrow(PageID left_page_id, PageID right_page_id);

    bool Merge(PageID left_page_id, PageID right_page_id, bool merge_into_left);
};

#endif  // DBS_TUTORIAL_INDEXFILE_H
