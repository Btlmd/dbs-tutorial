//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXPAGE_H
#define DBS_TUTORIAL_INDEXPAGE_H

#include <defines.h>
#include <io/BufferSystem.h>
#include <index/IndexRecord.h>
#include <index/IndexMeta.h>

/*
 ** Index Page Layout (Leaf)
 * | PageHeader |
 * | IndexRecord <0> | IndexRecord <1> | ... | IndexRecord <n-1> |
 * where IndexRecord <i> == (PageID <i>, SlotID <i>, Key <i>)
 *
 * Index Page Layout (Internal)
 * | PageHeader |
 * | IndexRecord <0> | IndexRecord <1> | ... | IndexRecord <n-1> |
 * where IndexRecord <i> == (PageID <i>, Key <i>)
 *
 * For every internal node <j>, we ensure that all elements in the subtree rooted at <j>
 * are less than or equal to Key <j>.
 *
 * The methods of IndexPage act like a vector. ()
 */

class IndexPage {
   public:
    class PageHeader {
       public:
        bool is_leaf;
        TreeOrder child_cnt;
        // Neighbors and parents. Returns -1 if not exist.
        PageID prev_page;
        PageID nxt_page;
        PageID parent_page;
    } &header;

    Page* page;
    const IndexMeta &meta;


    explicit IndexPage(Page *_page, const IndexMeta& _meta) : header{*reinterpret_cast<PageHeader *>(_page->data)},
                                                     page{_page}, meta{_meta} {}

    /* Start: Manipulation methods */
    [[nodiscard]] std::shared_ptr<IndexRecord> Select(TreeOrder slot) const;
    TreeOrder Insert(TreeOrder slot, std::shared_ptr<IndexRecord> record);
    void Update(TreeOrder slot, std::shared_ptr<IndexRecord> record);
    void Delete(TreeOrder slot_id);
    [[nodiscard]] std::vector<std::shared_ptr<IndexRecord>> SelectRange(TreeOrder start, TreeOrder end) const;
    void InsertRange(TreeOrder slot, std::vector<std::shared_ptr<IndexRecord>> records);
    void DeleteRange(TreeOrder start, TreeOrder end);  // Delete [start, end]
    /* End: Manipulation methods */

    void Init(bool is_leaf) {
        header.child_cnt = 0;
        header.is_leaf = is_leaf;
        header.prev_page = -1;
        header.nxt_page = -1;
        header.parent_page = -1;
    }

    RecordSize IndexRecordSize() const {
        return meta.Size(header.is_leaf);
    }

    bool IsLeaf() const {
        return header.is_leaf;
    }

    TreeOrder ChildCount() const {
        return header.child_cnt;
    }

    void SetChildCount(TreeOrder cnt) {
        header.child_cnt = cnt;
        page->SetDirty();
    }

    PageID PrevPage() const {
            return header.prev_page;
    }

    void SetPrevPage(PageID page_id) {
        header.prev_page = page_id;
        page->SetDirty();
    }

    PageID NextPage() const {
        return header.nxt_page;
    }

    void SetNextPage(PageID page_id) {
        header.nxt_page = page_id;
        page->SetDirty();
    }

    PageID ParentPage() const {
        return header.parent_page;
    }

    void SetParentPage(PageID page_id) {
        header.parent_page = page_id;
        page->SetDirty();
    }

    bool IsOverflow() {
        return header.child_cnt > meta.m;
    }

    bool IsUnderflow(bool is_root) {
        return header.child_cnt < (meta.m + 1) / 2 && !is_root;
    }

    std::shared_ptr<IndexRecord> CastRecord(std::shared_ptr<IndexRecord> ptr) {
        if (IsLeaf()) {
            return std::dynamic_pointer_cast<IndexRecordLeaf>(ptr);
        } else {
            auto _ptr = std::dynamic_pointer_cast<IndexRecordInternal>(ptr);
            if (_ptr) { return _ptr; }
            else {
                return std::dynamic_pointer_cast<IndexRecordLeaf>(ptr)->ToInternal();
            }
        }
    }

    void Print() {  // Used for debugging
        DebugLog << fmt::format("<Page {:03d}>[{}][↑{:03d}][←{:03d}][→{:03d}] ", page->id, IsLeaf() ? "L" : "I",
                                ParentPage(), PrevPage(), NextPage());
        for (TreeOrder i = 0; i < ChildCount(); ++i) {
            auto record = Select(i);
            record = CastRecord(record);
            if (IsLeaf()) {
                auto record2 = std::dynamic_pointer_cast<IndexRecordLeaf>(record);
                DebugLog << fmt::format("        <Page {:03d} Slot {:08d} Key {}> ", record2->page_id, record2->slot_id,
                                        record2->key->ToString());
            } else {
                DebugLog << fmt::format("        <Page {:03d} Key {}> ", record->page_id, record->key->ToString());
            }
        }
        DebugLog << std::endl;
    }

};

#endif  // DBS_TUTORIAL_INDEXPAGE_H
