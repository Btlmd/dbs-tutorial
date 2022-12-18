//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXPAGE_H
#define DBS_TUTORIAL_INDEXPAGE_H

#include <defines.h>
#include <io/BufferSystem.h>
#include <index/IndexMeta.h>

/*
 ** Index Page Layout (Leaf)
 * | PageHeader |
 * | Record <0> | Record <1> | ... | Record <n-1> |
 * where Record <i> == (PageID <i>, SlotID <i>, Key <i>)
 *
 * Index Page Layout (Internal)
 * | PageHeader |
 * | Record <0> | Record <1> | ... | Record <n-1> |
 * where Record <i> == (PageID <i>, Key <i>)
 *
 * For every internal node <j>, we ensure that all elements in the subtree rooted at <j>
 * are less than or equal to Key <j>.
 */



class IndexPage {
    class PageHeader {
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
};

#endif  // DBS_TUTORIAL_INDEXPAGE_H
