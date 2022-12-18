//
// Created by c7w on 2022/12/18.
//

#include "IndexRecord.h"

inline std::shared_ptr<IndexRecord> IndexRecord::FromSrc(
                    const uint8_t *&src, const IndexMeta& meta, bool is_leaf) {
    if (is_leaf) {
        return IndexRecordLeaf::FromSrc(src, meta);
    } else {
        return IndexRecordInternal::FromSrc(src, meta);
    }
}
