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

    [[nodiscard]] std::shared_ptr<IndexRecord> Select(TreeOrder slot) const;
    TreeOrder Insert(TreeOrder slot, std::shared_ptr<IndexRecord> record);
    void Delete(TreeOrder slot_id);

    void Init() {
        header.child_cnt = 0;
        header.is_leaf = true;
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
    }

    PageID PrevPage() const {
            return header.prev_page;
    }

    void SetPrevPage(PageID page_id) {
        header.prev_page = page_id;
    }

    PageID NextPage() const {
        return header.nxt_page;
    }

    void SetNextPage(PageID page_id) {
        header.nxt_page = page_id;
    }

    PageID ParentPage() const {
        return header.parent_page;
    }

    void SetParentPage(PageID page_id) {
        header.parent_page = page_id;
    }

    bool IsOverflow() {
        return header.child_cnt > meta.m;
    }

};

#endif  // DBS_TUTORIAL_INDEXPAGE_H
