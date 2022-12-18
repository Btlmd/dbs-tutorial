//
// Created by c7w on 2022/12/18.
//

#ifndef DBS_TUTORIAL_INDEXMETA_H
#define DBS_TUTORIAL_INDEXMETA_H

#include <defines.h>
#include <record/field.h>

class IndexMeta {
public:
    TreeOrder m;  // order of B+ tree,
    PageID page_num{1};  // number of pages in the index file
    PageID root_page{-1};  // -1 for empty tree
    FieldType field_type{FieldType::INT};  // Maybe I'd better write a IndexFieldType here...
};

#endif  // DBS_TUTORIAL_INDEXMETA_H
