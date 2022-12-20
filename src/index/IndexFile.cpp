//
// Created by c7w on 2022/12/18.
//

#include "IndexFile.h"

std::pair<PageID, TreeOrder> IndexFile::SelectRecord(const std::shared_ptr<IndexField>& key) {
    auto root_page_id = meta->root_page;
    if (root_page_id < 0) {
        return {-1, -1};  // Empty tree
    }

    std::pair<PageID, TreeOrder> ret = {-1, -1};

    PageID curr_page_id = root_page_id;
    while (!Page(curr_page_id)->IsLeaf()) {
        // Find the first child that is greater than or equal to key
        auto child_cnt = Page(curr_page_id)->ChildCount();
        auto curr_page_id_candidate = Page(curr_page_id)->Select(Page(curr_page_id)->ChildCount() - 1)->page_id;
        for (TreeOrder i = 0; i < child_cnt; ++i) {
            auto entry = Page(curr_page_id)->Select(i);
            auto entry_key = Page(curr_page_id)->CastRecord(entry)->key;
            if (*entry_key >= *key) {
                curr_page_id_candidate = entry->page_id;
                break;
            }
        }
        curr_page_id = curr_page_id_candidate;
    }
    ret.first = curr_page_id;

    // Now we find the leaf page, then we find the first record that is greater than or equal to key
    auto record_cnt = Page(curr_page_id)->ChildCount();
    ret.second = Page(curr_page_id)->ChildCount() - 1;  // For joint searching, it is possible that the record is not found
                                           // as we only consider the first key
    for (TreeOrder i = 0; i < record_cnt; ++i) {
        auto entry = Page(curr_page_id)->Select(i);
        if (*(entry->key) >= *key) {
            ret.second = i;
            break;
        }
    }
    DebugLog << fmt::format("SelectRecord: key = {}, page_id = {}, order = {}\n", key->ToString(), ret.first, ret.second);
    return ret;
}

void IndexFile::InsertRecord(PageID page_id, SlotID slot_id, const std::shared_ptr<IndexField>& key) {
    auto record = std::make_shared<IndexRecordLeaf>(page_id, slot_id, key);
    auto root_page_id = meta->root_page;
    if (root_page_id < 0) {
        // New page
        auto new_page = std::make_shared<IndexPage>(buffer.CreatePage(fd, PageNum()), *meta);
        auto new_page_id = new_page->page->id;
        meta->root_page = PageNum();
        ++meta->page_num;
        Page(new_page_id)->Init(true);
        Page(new_page_id)->Insert(0, record);
        return;
    }



    // 1.1: We find the leaf page
    auto [insert_page_id, insert_slot_id] {SelectRecord(key)};

    // 1.2: We follow the bottom link table to find a position (i, j) that satisfies:
    // (PageID i, SlotID j) is the first element that greater than or equal to key
    // Or (PageID i, SlotID j) marks the end of the bottom link table.
    PageID curr_page_id = insert_page_id;
    while (*(Page(curr_page_id)->Select(insert_slot_id)->key) < *key) {
        for (insert_slot_id = insert_slot_id + 1; insert_slot_id < Page(curr_page_id)->ChildCount(); ++insert_slot_id) {
            if (*(Page(curr_page_id)->Select(insert_slot_id)->key) >= *key) {
                break;
            }
        }

        if (insert_slot_id == Page(curr_page_id)->ChildCount()) {
            // The last element of the page is less than key, so we need to go to the next page
            if (Page(curr_page_id)->header.nxt_page < 0) {
                // We reached the end of the bottom link table
                // We just insert here
                break;
            } else {
                curr_page_id = Page(curr_page_id)->header.nxt_page;
                insert_slot_id = 0;
            }
        }
    }

    // 2. Now we find the position (i, j), we insert the record into the page i, slot j
    DebugLog << fmt::format("InsertRecord: key = {}, page_id = {}, order = {}\n", key->ToString(), insert_page_id, insert_slot_id);
    Page(curr_page_id)->Insert(insert_slot_id, record);

    // 3. If the page is overflow, we split it iteratively until it reaches to the root
    while (curr_page_id != -1) {
        if (Page(curr_page_id)->IsOverflow()) {
            if (Page(curr_page_id)->ParentPage() < 0) {
                // Case 3.1: Root Split
                // We create a new child page
                auto new_child_page_id = PageNum();
                std::make_shared<IndexPage>(buffer.CreatePage(fd, PageNum()), *meta);
                ++meta->page_num;
                Page(new_child_page_id)->Init(Page(curr_page_id)->IsLeaf());

                if (Page(curr_page_id)->IsLeaf()) {
                    Page(new_child_page_id)->SetPrevPage(curr_page_id);
                    Page(new_child_page_id)->SetNextPage(Page(curr_page_id)->NextPage());
                    Page(curr_page_id)->SetPrevPage(Page(curr_page_id)->PrevPage());
                    Page(curr_page_id)->SetNextPage(new_child_page_id);
                }

                Split(curr_page_id, new_child_page_id);  // Split children

                // We also need to create a new root page
                auto new_root_page_id = PageNum();
                std::make_shared<IndexPage>(buffer.CreatePage(fd, PageNum()), *meta);

                ++meta->page_num;
                Page(new_root_page_id)->Init(false);

                meta->root_page = new_root_page_id;
                Page(curr_page_id)->SetParentPage(new_root_page_id);
                Page(new_child_page_id)->SetParentPage(new_root_page_id);

                // Insert these two children into new root page
                std::shared_ptr<IndexField> _max_key = Page(curr_page_id)->Select(
                                                      Page(curr_page_id)->header.child_cnt - 1)->key;

                Page(new_root_page_id)->Insert(0,
                                             std::make_shared<IndexRecordInternal>(curr_page_id, _max_key));

                _max_key = Page(new_child_page_id)->Select(
                             Page(new_child_page_id)->header.child_cnt - 1)->key;
                Page(new_root_page_id)->Insert(1,
                                             std::make_shared<IndexRecordInternal>(new_child_page_id, _max_key));

            } else {
                // Case 3.2: Normal Split
                // We just split out a new page of the same type as curr_page, and insert it into the parent page
                auto new_child_page_id = PageNum();
                std::make_shared<IndexPage>(buffer.CreatePage(fd, PageNum()), *meta);
                ++meta->page_num;
                Page(new_child_page_id)->Init(Page(curr_page_id)->IsLeaf());

                if (Page(curr_page_id)->IsLeaf()) {
                    Page(new_child_page_id)->SetPrevPage(curr_page_id);
                    Page(new_child_page_id)->SetNextPage(Page(curr_page_id)->NextPage());
                    Page(curr_page_id)->SetPrevPage(Page(curr_page_id)->PrevPage());
                    Page(curr_page_id)->SetNextPage(new_child_page_id);
                }

                Split(curr_page_id, new_child_page_id);  // Split children

                Page(new_child_page_id)->SetParentPage(Page(curr_page_id)->ParentPage());

                // Insert the new child page into the parent page
                auto parent_page_id = Page(curr_page_id)->ParentPage();
                TreeOrder _insert_slot_id = Find(parent_page_id, curr_page_id);

                std::shared_ptr<IndexField> _max_key = Page(new_child_page_id)->Select(
                                                      Page(new_child_page_id)->header.child_cnt - 1)->key;
                Page(parent_page_id)->Insert(_insert_slot_id + 1,
                                             std::make_shared<IndexRecordInternal>(new_child_page_id, _max_key));

            }
        }

        // Update corresponding key in the parent page
        auto parent_page_id = Page(curr_page_id)->ParentPage();
        if (parent_page_id > 0) {
            TreeOrder _insert_slot_id = Find(parent_page_id, curr_page_id);

            std::shared_ptr<IndexField> _max_key = Page(curr_page_id)->Select(
                                                          Page(curr_page_id)->header.child_cnt - 1)->key;
            Page(parent_page_id)->Update(_insert_slot_id,
                                         std::make_shared<IndexRecordInternal>(curr_page_id, _max_key));
        }

        // Recursively search the parent page
        curr_page_id = Page(curr_page_id)->ParentPage();
    }

}
//void IndexFile::DeleteRecord(PageID page_id_old, SlotID slot_id_old, IndexField* key) {}
//void IndexFile::DeleteRecordRange(IndexField* key1, IndexField* key2) {}

