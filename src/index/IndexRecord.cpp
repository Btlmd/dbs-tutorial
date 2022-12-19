//
// Created by c7w on 2022/12/18.
//

#include "IndexRecord.h"

std::shared_ptr<IndexRecord> IndexRecord::FromSrc(
                    const uint8_t *&src, const IndexMeta& meta, bool is_leaf) {
    if (is_leaf) {
        return IndexRecordLeaf::FromSrc(src, meta);
    } else {
        return IndexRecordInternal::FromSrc(src, meta);
    }
}


std::shared_ptr<IndexRecordInternal> IndexRecordInternal::FromSrc(const uint8_t *&src, const IndexMeta &meta) {
    PageID page_id;
    read_var(src, page_id);
    auto key = IndexField::LoadIndexFieldShort(meta.field_type, src);
    return std::make_shared<IndexRecordInternal>(page_id, key);
}


std::shared_ptr<IndexRecordLeaf> IndexRecordLeaf::FromSrc(const uint8_t *&src, const IndexMeta &meta) {
    PageID page_id;
    read_var(src, page_id);
    SlotID slot_id;
    read_var(src, slot_id);
    auto key = IndexField::LoadIndexField(meta.field_type, src);
    return std::make_shared<IndexRecordLeaf>(page_id, slot_id, key);
}
