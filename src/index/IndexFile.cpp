//
// Created by c7w on 2022/12/18.
//

#include "IndexFile.h"

std::pair<PageID, TreeOrder> IndexFile::SelectRecord(IndexField* key) {
    auto root_page_id = meta->root_page;
    if (root_page_id < 0) {
        return {-1, -1};  // Empty tree
    }

    std::shared_ptr<IndexPage> curr_page = nullptr;
    auto ReadPage = [&](PageID page_id) {
        curr_page = std::make_shared<IndexPage>(buffer.ReadPage(fd, page_id), *meta);
    };

    std::pair<PageID, TreeOrder> ret = {-1, -1};

    ReadPage(root_page_id);
    while (!curr_page->IsLeaf()) {
        // Find the first child that is greater than or equal to key
        auto child_cnt = curr_page->ChildCount();
        for (TreeOrder i = 0; i < child_cnt; ++i) {
            auto entry = curr_page->Select(i);
            if (*(entry->key) >= *key) {
                ReadPage(entry->page_id);
                ret.first = entry->page_id;
                break;
            }
        }
    }

    // Now we find the leaf page, then we find the first record that is greater than or equal to key
    auto record_cnt = curr_page->ChildCount();
    ret.second = curr_page->ChildCount() - 1;  // For joint searching, it is possible that the record is not found
                                           // as we only consider the first key
    for (TreeOrder i = 0; i < record_cnt; ++i) {
        auto entry = curr_page->Select(i);
        if (*(entry->key) >= *key) {
            ret.second = i;
            break;
        }
    }

    return ret;
}

void IndexFile::InsertRecord(PageID page_id, SlotID slot_id, IndexField* key) {
    auto record = std::make_shared<IndexRecordLeaf>(page_id, slot_id, key);
    auto root_page_id = meta->root_page;
    if (root_page_id < 0) {
        // New page
        auto new_page = std::make_shared<IndexPage>(buffer.ReadPage(fd, PageNum()), *meta);
        meta->root_page = PageNum();
        ++meta->page_num;
        new_page->Init();
        new_page->Insert(0, record);
        new_page->page->SetDirty();
        return;
    }

    std::shared_ptr<IndexPage> curr_page = nullptr;
    auto ReadPage = [&](PageID page_id) {
        curr_page = std::make_shared<IndexPage>(buffer.ReadPage(fd, page_id), *meta);
    };

    // 1.1: We find the leaf page
    auto [insert_page_id, insert_slot_id] {SelectRecord(key)};

    // 1.2: We follow the bottom link table to find a position (i, j) that satisfies:
    // (PageID i, SlotID j) is the first element that greater than or equal to key
    // Or (PageID i, SlotID j) marks the end of the bottom link table.
    ReadPage(insert_page_id);
    while (curr_page->Select(insert_slot_id)->key < key) {
        for (insert_slot_id = insert_slot_id + 1; insert_slot_id < curr_page->ChildCount(); ++insert_slot_id) {
            if (curr_page->Select(insert_slot_id)->key >= key) {
                break;
            }
        }
        if (insert_slot_id == curr_page->ChildCount()) {
            // The last element of the page is less than key, so we need to go to the next page
            if (curr_page->header.nxt_page < 0) {
                // We reached the end of the bottom link table
                // We just insert here
                break;
            } else {
                insert_page_id = curr_page->header.nxt_page;
                ReadPage(insert_page_id);
                insert_slot_id = 0;
            }
        }
    }

    // 2. Now we find the position (i, j), we insert the record into the page i, slot j
    curr_page->Insert(insert_slot_id, record);

    // 3. If the page is overflow, we split it iteratively until it reaches to the root
    auto curr_page_id = insert_page_id;
    while (curr_page_id != root_page_id) {
        if (curr_page->IsOverflow()) {
            if (curr_page->ParentPage() < 0) {
                // Case 3.1: Root Split
                // We create a new leaf page
                auto new_leaf_page = std::make_shared<IndexPage>(buffer.ReadPage(fd, PageNum()), *meta);
                auto new_leaf_page_id = PageNum();
                ++meta->page_num;
                new_leaf_page->Init();
                // TODO

                // We change the properties of curr_page
                // TODO

                // We also need to create a new root page
                auto new_root_page = std::make_shared<IndexPage>(buffer.ReadPage(fd, PageNum()), *meta);
                auto new_root_page_id = PageNum();
                ++meta->page_num;
                new_root_page->Init(); new_root_page->header.is_leaf = false;
                // TODO: do something to the new root page



            } else {
                // Case 3.2: Normal Split
                // We just split out a new page, and insert it into the parent page

            }


        }
    }





}

void IndexFile::DeleteRecord(PageID page_id, SlotID slot_id, IndexField* key) {

}
