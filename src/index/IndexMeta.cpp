//
// Created by c7w on 2022/12/18.
//

#include "IndexMeta.h"

RecordSize IndexMeta::Size(bool is_leaf) const {
    // Construct a null object of field_type
    auto null_field{IndexField::MakeNull(field_type)};
    // Return its size
    if (is_leaf) {
        return null_field->Size();
    } else {
        return null_field->ShortSize();
    }
}
