//
// Created by c7w on 2022/12/18.
//

#include "IndexMeta.h"

RecordSize IndexMeta::Size(bool is_leaf) {

    if (is_leaf && _size_leaf > 0) {
        return _size_leaf;
    } else if (!is_leaf && _size_internal > 0) {
        return _size_internal;
    }

    // Construct a null object of field_type
    auto null_field{IndexField::MakeNull(field_type)};

    // Return the size of index record
    if (is_leaf) {
        auto index_record = std::make_shared<IndexRecordLeaf>(0, 0, null_field);
        return _size_leaf = index_record->Size();
    } else {
        auto index_record = std::make_shared<IndexRecordInternal>(0, null_field);
        return _size_internal = index_record->Size();
    }



}
