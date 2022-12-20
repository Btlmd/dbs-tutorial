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
    auto insert_slot_id_candidate = Page(curr_page_id)->ChildCount() - 1;  // For joint searching, it is possible that the record is not found
                                           // as we only consider the first key
    for (TreeOrder i = 0; i < record_cnt; ++i) {
        auto entry = Page(curr_page_id)->Select(i);
        if (*(entry->key) >= *key) {
            insert_slot_id_candidate = i;
            break;
        }
    }
    auto insert_slot_id = insert_slot_id_candidate;

    // Then we follow the bottom link table to find the first record that is greater than or equal to key
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

    ret = {curr_page_id, insert_slot_id};

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

    // 2. Now we find the position (i, j), we insert the record into the page i, slot j
    DebugLog << fmt::format("InsertRecord: key = {}, page_id = {}, order = {}\n", key->ToString(), insert_page_id, insert_slot_id);
    PageID curr_page_id = insert_page_id;
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

                Page(new_child_page_id)->SetPrevPage(curr_page_id);
                Page(new_child_page_id)->SetNextPage(Page(curr_page_id)->NextPage());
                Page(curr_page_id)->SetPrevPage(Page(curr_page_id)->PrevPage());
                Page(curr_page_id)->SetNextPage(new_child_page_id);

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


                Page(new_child_page_id)->SetPrevPage(curr_page_id);
                Page(new_child_page_id)->SetNextPage(Page(curr_page_id)->NextPage());
                Page(curr_page_id)->SetPrevPage(Page(curr_page_id)->PrevPage());
                Page(curr_page_id)->SetNextPage(new_child_page_id);


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


int IndexFile::DeleteRecord(PageID page_id, SlotID slot_id, const std::shared_ptr<IndexField>& key) {

    auto record = std::make_shared<IndexRecordLeaf>(page_id, slot_id, key);
    auto root_page_id = meta->root_page;
    if (root_page_id < 0) {
        return 0;  // Delete failed
    }

    // 1. Find the position of the record
    auto [delete_page_id, delete_slot_id] {SelectRecord(key)};

    // 2. As by (PageID, SlotID) we can uniquely identify a record, we only need to check the first element of the pair
    if ( delete_slot_id < Page(delete_page_id)->ChildCount()) {
        auto delete_record_candidate = Page(delete_page_id)->Select(delete_slot_id);
        // Cast to IndexRecordLeaf
        auto delete_record = std::dynamic_pointer_cast<IndexRecordLeaf>(delete_record_candidate);
        if (delete_record->page_id == page_id && delete_record->slot_id == slot_id && delete_record->key == key) {
            // 3. Delete the record
            Page(delete_page_id)->Delete(delete_slot_id);
            SolveDelete(delete_page_id);
            return 1;  // Delete success
        }
    }

    return 0;  // Delete failed
}

int IndexFile::DeleteRecordRange(const std::shared_ptr<IndexField>& key1, const std::shared_ptr<IndexField>& key2) {
    // Delete all elements that satisfies key1 <= key <= key2
    auto record1 = std::make_shared<IndexRecordLeaf>(0, 0, key1);
    auto record2 = std::make_shared<IndexRecordLeaf>(0, 0, key2);

    auto root_page_id = meta->root_page;
    if (root_page_id < 0) {
        return 0;  // Delete failed
    }

    // 1. Find the position of the record
    auto [delete_page_id, delete_slot_id] {SelectRecord(key1)};

    // 2. Iterate through the bottom link table and delete all records that satisfies key1 <= key <= key2
    // Use page->DeleteRange() to delete a range of records
    int delete_cnt = 0;
    while (delete_page_id > 0) {
        std::pair<TreeOrder, TreeOrder> curr_delete_range = {Page(delete_page_id)->ChildCount(), (TreeOrder) 0};
        auto child_cnt = Page(delete_page_id)->ChildCount();
        while (delete_slot_id < child_cnt) {
            // Find the range that satisfies key1 <= key <= key2
            auto delete_record_candidate = Page(delete_page_id)->Select(delete_slot_id);
            // Cast to IndexRecordLeaf
            if (*(delete_record_candidate->key) >= *key1 && *(delete_record_candidate->key) <= *key2) {
                curr_delete_range.first = std::min(curr_delete_range.first, delete_slot_id);
                curr_delete_range.second = std::max(curr_delete_range.second, delete_slot_id);
            }
            ++delete_slot_id;
        }

        // Judge whether the range is valid
        if (curr_delete_range.first <= curr_delete_range.second) {
            Page(delete_page_id)->DeleteRange(curr_delete_range.first, curr_delete_range.second);
            delete_cnt += curr_delete_range.second - curr_delete_range.first + 1;
            delete_page_id = SolveDelete(delete_page_id);  // In SolveDelete, you MUST assure that curr_page is not deleted
        } else {
            // If the first element of the page is larger than the second element, then the range is invalid
            if (*(Page(delete_page_id)->Select(0)->key) > *key2) {
                break;
            }
            delete_page_id = Page(delete_page_id)->NextPage();
        }

        delete_slot_id = 0;
    }

    return delete_cnt;
}

// Returns the next valid page after deletion for searching (here `next` means it is possible to meet new elements)
// In case of not underflow, return the next page
// In case of borrowing, return the current page
// In case of merging, return the next page
PageID IndexFile::SolveDelete(PageID delete_page_id) {
    // Recursively update key
    auto UpdateKey = [&](PageID page_id) {
        auto curr_page_id = page_id;
        while (curr_page_id > 0) {
            auto parent_page_id = Page(curr_page_id)->ParentPage();
            if (parent_page_id > 0) {
                TreeOrder _slot_id = Find(parent_page_id, curr_page_id);
                std::shared_ptr<IndexField> _max_key = Page(curr_page_id)->Select(
                                                              Page(curr_page_id)->header.child_cnt - 1)->key;
                Page(parent_page_id)->Update(_slot_id,
                                             std::make_shared<IndexRecordInternal>(curr_page_id, _max_key));
            }

            // Recursively search the parent page
            curr_page_id = Page(curr_page_id)->ParentPage();
        }
    };

    auto DeleteKey = [&](PageID parent_page_id, PageID curr_page_id) {
        auto _slot_id = Find(parent_page_id, curr_page_id);
        Page(parent_page_id)->Delete(_slot_id);
    };

    PageID nxt_valid_page = Page(delete_page_id)->NextPage();
    while (Page(delete_page_id)->IsUnderflow(delete_page_id == meta->root_page)) {
        if (Borrow(Page(delete_page_id)->PrevPage(), delete_page_id)) {
            // 1. Try to borrow from the left sibling
            UpdateKey(Page(delete_page_id)->PrevPage());
            UpdateKey(delete_page_id);
            if (Page(delete_page_id)->IsLeaf()) nxt_valid_page = delete_page_id;
            break;
        } else if (Borrow(delete_page_id, Page(delete_page_id)->NextPage())) {
            // 2. Try to borrow from the right sibling
            UpdateKey(delete_page_id);
            UpdateKey(Page(delete_page_id)->NextPage());
            if (Page(delete_page_id)->IsLeaf()) nxt_valid_page = delete_page_id;
            break;
        } else if (Merge(Page(delete_page_id)->PrevPage(), delete_page_id, true)) {
            // 3. Try to merge curr_page into the left sibling
            UpdateKey(Page(delete_page_id)->PrevPage());
            DeleteKey(Page(delete_page_id)->ParentPage(), delete_page_id);
            delete_page_id = Page(delete_page_id)->ParentPage();
            // FreePage(delete_page_id);
        } else if (Merge(delete_page_id, Page(delete_page_id)->NextPage(), false)) {
            // 4. Try to merge curr_page into the right sibling
            UpdateKey(Page(delete_page_id)->NextPage());
            DeleteKey(Page(delete_page_id)->ParentPage(), delete_page_id);
            delete_page_id = Page(delete_page_id)->ParentPage();
            // FreePage(delete_page_id);
        } else {
            // 5. If all the above operations fail, then the tree is invalid...
            // We should never reach here...
            throw std::runtime_error("Invalid B+ tree");
        }
    }

    UpdateKey(delete_page_id);

    if (meta->root_page > 0 && Page(meta->root_page)->ChildCount() == 1) {
        // If the root page has only one child, then we can delete the root page
        // FreePage(meta->root_page);
        auto new_root_page_id = Page(meta->root_page)->Select(0)->page_id;
        Page(new_root_page_id)->SetParentPage(-1);
        meta->root_page = new_root_page_id;
    }

    // Delete root if it is empty
    if (meta->root_page > 0 && Page(meta->root_page)->ChildCount() == 0) {
        // FreePage(meta->root_page);
        meta->root_page = -1;
    }

    return nxt_valid_page;
}

bool IndexFile::Borrow(PageID left_page_id, PageID right_page_id) {
    if (left_page_id <= 0 || right_page_id <= 0) {
        return false;
    }

    auto left_page_child_cnt = Page(left_page_id)->ChildCount();
    auto right_page_child_cnt = Page(right_page_id)->ChildCount();

    if (left_page_child_cnt + right_page_child_cnt < (meta->m + 1) / 2 * 2) {
        return false;
    }

    // Borrow!
    auto left_page_new_child_cnt = (left_page_child_cnt + right_page_child_cnt) / 2;
    auto right_page_new_child_cnt = left_page_child_cnt + right_page_child_cnt - left_page_new_child_cnt;

    // 1. Move records from left page to right page
    if (left_page_child_cnt - left_page_new_child_cnt > 0) {
        Page(right_page_id)->InsertRange(0, Page(left_page_id)->SelectRange(left_page_new_child_cnt, left_page_child_cnt - 1));
        Page(left_page_id)->DeleteRange(left_page_new_child_cnt, left_page_child_cnt - 1);
    }

    // 2. Move records from right page to left page
    if (right_page_child_cnt - right_page_new_child_cnt > 0) {
            Page(left_page_id)->InsertRange(left_page_child_cnt,
                                        Page(right_page_id)->SelectRange(0, right_page_child_cnt - right_page_new_child_cnt - 1));
            Page(right_page_id)->DeleteRange(0, right_page_child_cnt - right_page_new_child_cnt - 1);
    }

    // 3. Update the parent page of children
    for (auto i = 0; i < Page(left_page_id)->ChildCount(); ++i) {
        auto child_page_id = Page(left_page_id)->Select(i)->page_id;
        Page(child_page_id)->SetParentPage(left_page_id);
    }

    for (auto i = 0; i < Page(right_page_id)->ChildCount(); ++i) {
        auto child_page_id = Page(right_page_id)->Select(i)->page_id;
        Page(child_page_id)->SetParentPage(right_page_id);
    }

    return true;
}

bool IndexFile::Merge(PageID left_page_id, PageID right_page_id, bool merge_into_left) {
    if (left_page_id <= 0 || right_page_id <= 0) {
        return false;
    }

    auto left_page_child_cnt = Page(left_page_id)->ChildCount();
    auto right_page_child_cnt = Page(right_page_id)->ChildCount();

    if (left_page_child_cnt + right_page_child_cnt > meta->m) {
        return false;
    }

    // Merge!
    if (merge_into_left) {
        Page(left_page_id)->InsertRange(left_page_child_cnt, Page(right_page_id)->SelectRange(0, right_page_child_cnt - 1));
        for (auto i = 0; i < Page(right_page_id)->ChildCount(); ++i) {
            auto child_page_id = Page(right_page_id)->Select(i)->page_id;
            Page(child_page_id)->SetParentPage(left_page_id);
        }
        Page(left_page_id)->SetNextPage(Page(right_page_id)->NextPage());
        if (Page(right_page_id)->NextPage() > 0) {
            Page(Page(right_page_id)->NextPage())->SetPrevPage(left_page_id);
        }
    } else {
        Page(right_page_id)->InsertRange(0, Page(left_page_id)->SelectRange(0, left_page_child_cnt - 1));
        for (auto i = 0; i < Page(left_page_id)->ChildCount(); ++i) {
            auto child_page_id = Page(left_page_id)->Select(i)->page_id;
            Page(child_page_id)->SetParentPage(right_page_id);
        }
        Page(right_page_id)->SetPrevPage(Page(left_page_id)->PrevPage());
        if (Page(left_page_id)->PrevPage() > 0) {
            Page(Page(left_page_id)->PrevPage())->SetNextPage(right_page_id);
        }
    }

    return true;
}
